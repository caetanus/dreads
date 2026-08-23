package dreadsconf

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/kafka"
	"github.com/faustbrian/golib/pkg/kafka/kafkatest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func brokers() []string { return []string{"127.0.0.1:19092"} }

var topicSeq atomic.Uint64

func makeHarness(t *testing.T) kafkatest.BrokerHarness {
	bs := brokers()
	return kafkatest.BrokerHarness{
		Brokers:  bs,
		Security: kafka.DevelopmentPlaintextSecurity(),
		// dreads topics auto-exist with KAFKA_PARTITIONS; no create needed.
		NewTopic: func(tb *testing.T, partitions int) string {
			tb.Helper()
			name := fmt.Sprintf("golib-conf-%d-%d", time.Now().UnixNano(), topicSeq.Add(1))
			// registry mode (DREADS_KAFKA_AUTOCREATE=false) needs the topic created
			cl, err := kgo.NewClient(kgo.SeedBrokers(bs...))
			if err == nil {
				kadm.NewClient(cl).CreateTopics(tb.Context(), int32(partitions), 1, nil, name)
				cl.Close()
			}
			return name
		},
		ReadRecords: func(ctx context.Context, req kafkatest.ReadRequest) ([]kafka.ConsumedRecord, error) {
			client, err := kgo.NewClient(
				kgo.SeedBrokers(bs...),
				kgo.ClientID("dreads-conf-reader"),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					req.Topic: {req.Partition: kgo.NewOffset().At(req.StartOffset)},
				}),
				kgo.DialTimeout(10*time.Second),
			)
			if err != nil {
				return nil, err
			}
			defer client.Close()
			out := make([]kafka.ConsumedRecord, 0, req.MaxRecords)
			deadline := time.Now().Add(20 * time.Second)
			for len(out) < req.MaxRecords && time.Now().Before(deadline) {
				f := client.PollRecords(ctx, req.MaxRecords-len(out))
				if err := f.Err(); err != nil {
					return nil, err
				}
				for _, r := range f.Records() {
					if r.Topic != req.Topic || r.Partition != req.Partition || r.Offset < req.StartOffset {
						continue
					}
					hs := make([]kafka.Header, len(r.Headers))
					for i := range r.Headers {
						hs[i] = kafka.Header{Key: r.Headers[i].Key, Value: append([]byte(nil), r.Headers[i].Value...)}
					}
					out = append(out, kafka.ConsumedRecord{
						Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
						Key: append([]byte(nil), r.Key...), Value: append([]byte(nil), r.Value...),
						Headers: hs, Timestamp: r.Timestamp,
						TimestampType: kafka.TimestampType(r.Attrs.TimestampType()),
						LeaderEpoch:   r.LeaderEpoch,
					})
				}
			}
			return out, nil
		},
		// dreads has no consumer groups; a missing commit returns -1 per contract.
		CommittedOffset: func(ctx context.Context, group, topic string, partition int32) (int64, error) {
			client, err := kgo.NewClient(kgo.SeedBrokers(bs...), kgo.DialTimeout(10*time.Second))
			if err != nil {
				return -1, nil
			}
			defer client.Close()
			offs, err := kadm.NewClient(client).FetchOffsets(ctx, group)
			if err != nil {
				return -1, nil
			}
			o, ok := offs.Lookup(topic, partition)
			if !ok || o.Err != nil {
				return -1, nil
			}
			return o.At, nil
		},
	}
}

func TestDreadsProducerConformance(t *testing.T)    { kafkatest.RunProducerConformance(t, makeHarness(t)) }
func TestDreadsReplayConformance(t *testing.T)      { kafkatest.RunReplayConformance(t, makeHarness(t)) }
func TestDreadsInspectorConformance(t *testing.T)   { kafkatest.RunInspectorConformance(t, makeHarness(t)) }
func TestDreadsConsumerConformance(t *testing.T)    { kafkatest.RunConsumerConformance(t, makeHarness(t)) }
func TestDreadsTransactionConformance(t *testing.T) { kafkatest.RunTransactionConformance(t, makeHarness(t)) }
