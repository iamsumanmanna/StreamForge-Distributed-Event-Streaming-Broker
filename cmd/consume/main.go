package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-system/streamforge/internal/transport/protocol"
	"github.com/distributed-system/streamforge/pkg/client"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9092", "broker address")
	topic := flag.String("topic", "test", "topic")
	partition := flag.Int("partition", -1, "partition (-1 = use consumer group)")
	offset := flag.Int64("offset", 0, "start offset (ignored if group commits exist)")
	groupID := flag.String("group", "", "consumer group id")
	fromBeginning := flag.Bool("from-beginning", false, "start at 0 even with group")
	tlsOn := flag.Bool("tls", false, "use TLS")
	insecure := flag.Bool("tls-insecure", true, "skip TLS verify")
	token := flag.String("auth-token", "", "auth token")
	flag.Parse()

	c, err := client.DialWithOptions(*addr, client.DialOptions{
		TLS: *tlsOn, Insecure: *insecure, AuthToken: *token,
	})
	if err != nil {
		fatal(err)
	}
	defer c.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	if *groupID != "" {
		runGroup(c, *topic, *groupID, *fromBeginning, sig)
		return
	}
	part := int32(*partition)
	if part < 0 {
		part = 0
	}
	runPartition(c, *topic, part, *offset, sig)
}

func runPartition(c *client.Client, topic string, partition int32, offset int64, sig <-chan os.Signal) {
	for {
		select {
		case <-sig:
			return
		default:
		}
		resp, err := c.Fetch(topic, partition, offset, 1<<20)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			time.Sleep(time.Second)
			continue
		}
		for _, r := range resp.Records {
			fmt.Printf("partition=%d offset=%d key=%s value=%s\n", partition, r.Offset, r.Key, r.Value)
			offset = r.Offset + 1
		}
		if len(resp.Records) == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func runGroup(c *client.Client, topic, groupID string, fromBeginning bool, sig <-chan os.Signal) {
	join, err := c.JoinGroup(groupID, "", []string{topic})
	if err != nil {
		fatal(err)
	}
	fmt.Printf("joined group member=%s gen=%d leader=%s members=%v\n", join.MemberID, join.GenerationID, join.LeaderID, join.Members)

	var assignments map[string][]protocol.Assignment
	if join.MemberID == join.LeaderID {
		assignments = nil // coordinator auto range-assigns when leader sends empty
	}
	syncResp, err := c.SyncGroup(protocol.SyncGroupReq{
		GroupID: groupID, MemberID: join.MemberID, GenerationID: join.GenerationID, Assignments: assignments,
	})
	if err != nil {
		fatal(err)
	}
	fmt.Printf("assigned %v\n", syncResp.Assignments)

	offsets := map[string]int64{}
	if !fromBeginning {
		fo, err := c.FetchOffsets(groupID, topic)
		if err == nil {
			for _, e := range fo.Offsets {
				offsets[fmt.Sprintf("%s-%d", topic, e.Partition)] = e.Offset
			}
		}
	}

	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for range t.C {
			_ = c.Heartbeat(groupID, join.MemberID, join.GenerationID)
		}
	}()

	for {
		select {
		case <-sig:
			_ = c.LeaveGroup(groupID, join.MemberID)
			return
		default:
		}
		progress := false
		for _, a := range syncResp.Assignments {
			key := fmt.Sprintf("%s-%d", a.Topic, a.Partition)
			off := offsets[key]
			resp, err := c.Fetch(a.Topic, a.Partition, off, 1<<20)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			for _, r := range resp.Records {
				fmt.Printf("partition=%d offset=%d key=%s value=%s\n", a.Partition, r.Offset, r.Key, r.Value)
				off = r.Offset + 1
				progress = true
			}
			offsets[key] = off
			_ = c.CommitOffset(groupID, a.Topic, a.Partition, off)
		}
		if !progress {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
