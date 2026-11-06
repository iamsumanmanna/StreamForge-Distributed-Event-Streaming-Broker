package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"

	"github.com/distributed-system/streamforge/internal/storage/compress"
	"github.com/distributed-system/streamforge/pkg/client"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9092", "broker address")
	topic := flag.String("topic", "test", "topic")
	partitions := flag.Int("partitions", 1, "create topic with N partitions if needed")
	key := flag.String("key", "", "message key")
	value := flag.String("value", "", "message value")
	count := flag.Int("count", 1, "number of messages")
	acks := flag.Int("acks", 1, "acks: 0, 1, or -1 (all)")
	comp := flag.String("compression", "none", "none|gzip|snappy|lz4|zstd")
	tlsOn := flag.Bool("tls", false, "use TLS")
	insecure := flag.Bool("tls-insecure", true, "skip TLS verify")
	token := flag.String("auth-token", "", "auth token")
	flag.Parse()

	if *value == "" && *count == 1 {
		fmt.Fprintln(os.Stderr, "provide -value or use -count with generated values")
		os.Exit(1)
	}

	codec, err := compress.Parse(*comp)
	if err != nil {
		fatal(err)
	}

	c, err := client.DialWithOptions(*addr, client.DialOptions{
		TLS: *tlsOn, Insecure: *insecure, AuthToken: *token,
	})
	if err != nil {
		fatal(err)
	}
	defer c.Close()

	if err := c.CreateTopic(*topic, int32(*partitions)); err != nil {
		fatal(err)
	}

	for i := 0; i < *count; i++ {
		v := *value
		if v == "" {
			v = fmt.Sprintf("msg-%d", i)
		}
		k := []byte(*key)
		if *count > 1 && *key == "" {
			k = []byte(fmt.Sprintf("k-%d", i))
		}
		part := int32(0)
		if len(k) > 0 && *partitions > 0 {
			h := fnv.New32a()
			_, _ = h.Write(k)
			part = int32(h.Sum32() % uint32(*partitions))
		}
		off, err := c.ProduceOpts(*topic, part, k, []byte(v), int16(*acks), codec)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("produced topic=%s partition=%d offset=%d acks=%d compression=%s\n",
			*topic, part, off, *acks, compress.Name(codec))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
