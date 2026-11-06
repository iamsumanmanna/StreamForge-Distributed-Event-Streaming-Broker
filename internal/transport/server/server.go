package server

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/distributed-system/streamforge/internal/broker"
	"github.com/distributed-system/streamforge/internal/transport/protocol"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server accepts TCP protocol connections and serves HTTP metrics.
type Server struct {
	broker   *broker.Broker
	listen   string
	httpAddr string
	tlsCert  string
	tlsKey   string
	log      *slog.Logger
	ln       net.Listener
	httpSrv  *http.Server
	wg       sync.WaitGroup
}

func New(b *broker.Broker, listen, httpAddr string, logger *slog.Logger) *Server {
	return &Server{broker: b, listen: listen, httpAddr: httpAddr, log: logger}
}

// SetTLS enables TLS on the broker TCP listener.
func (s *Server) SetTLS(certFile, keyFile string) {
	s.tlsCert = certFile
	s.tlsKey = keyFile
}

func (s *Server) Start() error {
	var ln net.Listener
	var err error
	if s.tlsCert != "" && s.tlsKey != "" {
		cert, err := tls.LoadX509KeyPair(s.tlsCert, s.tlsKey)
		if err != nil {
			return err
		}
		ln, err = tls.Listen("tcp", s.listen, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
		if err != nil {
			return err
		}
		s.log.Info("TLS enabled for broker TCP")
	} else {
		ln, err = net.Listen("tcp", s.listen)
		if err != nil {
			return err
		}
	}
	s.ln = ln
	s.listen = ln.Addr().String()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	httpLn, err := net.Listen("tcp", s.httpAddr)
	if err != nil {
		_ = ln.Close()
		return err
	}
	s.httpAddr = httpLn.Addr().String()
	s.httpSrv = &http.Server{Handler: mux}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpSrv.Serve(httpLn); err != nil && err != http.ErrServerClosed {
			s.log.Error("http server", "err", err)
		}
	}()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop()
	}()
	s.log.Info("broker listening", "tcp", s.listen, "http", s.httpAddr)
	return nil
}

// Addr returns the bound TCP broker address.
func (s *Server) Addr() string { return s.listen }

// HTTPAddr returns the bound HTTP address.
func (s *Server) HTTPAddr() string { return s.httpAddr }

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	needAuth := s.broker.AuthToken() != ""
	authed := !needAuth
	for {
		frame, err := protocol.DecodeFrame(conn)
		if err != nil {
			return
		}
		h, payload, err := protocol.DecodeRequest(frame)
		if err != nil {
			return
		}
		if needAuth && !authed {
			if h.APIKey != protocol.APIAuth {
				resp := protocol.EncodeResponse(h.CorrelationID, protocol.ErrUnauthorized, nil)
				_ = protocol.EncodeFrame(conn, resp)
				return
			}
			errCode, respPayload := s.broker.Handle(h.APIKey, payload)
			resp := protocol.EncodeResponse(h.CorrelationID, errCode, respPayload)
			if err := protocol.EncodeFrame(conn, resp); err != nil {
				return
			}
			if errCode == protocol.ErrNone {
				authed = true
			} else {
				return
			}
			continue
		}
		errCode, respPayload := s.broker.Handle(h.APIKey, payload)
		resp := protocol.EncodeResponse(h.CorrelationID, errCode, respPayload)
		if err := protocol.EncodeFrame(conn, resp); err != nil {
			return
		}
	}
}

func (s *Server) Stop(ctx context.Context) error {
	if s.ln != nil {
		_ = s.ln.Close()
	}
	if s.httpSrv != nil {
		_ = s.httpSrv.Shutdown(ctx)
	}
	s.wg.Wait()
	return nil
}
