package dreadsconf

import (
	"bytes"
	"testing"
	"time"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLz4RoundTrip(t *testing.T) {
	topic := "lz4_rt"
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19092"),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil { t.Fatalf("client: %v", err) }
	want := []string{}
	val := bytes.Repeat([]byte("lz4-compressible-payload-"), 30)
	for i := 0; i < 5; i++ {
		v := append(append([]byte{}, byte('A'+i)), val...)
		want = append(want, string(v))
		if e := cl.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Partition: 0, Value: v}).FirstErr(); e != nil {
			t.Fatalf("produce %d: %v", i, e)
		}
	}
	cl.Close()
	cc, _ := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:19092"),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(0)}}))
	defer cc.Close()
	got := []string{}
	deadline := time.Now().Add(8 * time.Second)
	for len(got) < 5 && time.Now().Before(deadline) {
		fs := cc.PollRecords(t.Context(), 5-len(got))
		if fs.Err() != nil { t.Fatalf("poll: %v", fs.Err()) }
		for _, r := range fs.Records() { got = append(got, string(r.Value)) }
	}
	if len(got) != 5 { t.Fatalf("got %d records, want 5", len(got)) }
	for i := range want {
		if got[i] != want[i] { t.Fatalf("record %d mismatch", i) }
	}
	t.Logf("lz4 round-trip OK: 5 lz4-frame records produced + consumed, values intact")
}
