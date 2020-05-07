package kafka_test

import (
	"github.com/stretchr/testify/assert"
	"go-mq/kafka"
	"go-mq/util"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
)

type MockProducer struct{}

func (MockProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return 0, 1, nil
}

func (MockProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	panic("implement me")
}

func (MockProducer) Close() error {
	panic("implement me")
}

type MockAsyncProducer struct {
	messageChan chan *sarama.ProducerMessage
	successChan chan *sarama.ProducerMessage
}

func (m *MockAsyncProducer) AsyncClose() {
	panic("implement me")
}

func (m *MockAsyncProducer) Close() error {
	panic("implement me")
}

func (m *MockAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return m.messageChan
}

func (m *MockAsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return m.successChan
}

func (m *MockAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return nil
}

var kf *kafka.Kafka

func TestMain(m *testing.M) {
	kafka, err := kafka.InitKafkaFromPath("../conf/kafka.yml")
	if err != nil {
		panic(err)
	}
	kf = kafka
	m.Run()
}

func Benchmark_SyncPush(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = kf.SyncPush("myPusher", util.MSG{
			KafkaMsg: util.KafkaMessage{
				ProducerMsg: &sarama.ProducerMessage{
					Topic: "test",
					Value: sarama.ByteEncoder([]byte(strconv.Itoa(i))),
				},
			},
		})
	}
}

func TestKafka_SyncPush(t *testing.T) {
	push := kf.SyncPush("myPusher", util.MSG{
		KafkaMsg: util.KafkaMessage{
			ProducerMsg: &sarama.ProducerMessage{
				Topic: "test",
				Value: sarama.ByteEncoder([]byte("ddddddddd")),
			},
		},
	})

	assert.Nil(t, push)
}

func TestKafka_OneWay(t *testing.T) {
	push := kf.AsyncPush("myPusher", util.MSG{
		KafkaMsg: util.KafkaMessage{
			ProducerMsg: &sarama.ProducerMessage{
				Topic: "test",
				Value: sarama.ByteEncoder([]byte("ddddddddd")),
			},
		},
	}, nil)

	assert.Nil(t, push)
}

func TestKafka_ASyncPush(t *testing.T) {
	var count int32 = 0
	var total int32 = 100
	for i := 0; int32(i) < total; i++ {
		//producer := kafka.AsyncPusherPool["myPusher"]
		//producer.(*MockAsyncProducer).successChan <- &sarama.ProducerMessage{
		//	Value:    sarama.ByteEncoder([]byte(fmt.Sprintf("send async message：%d", i))),
		//	Metadata: strconv.Itoa(i),
		//}
		push := kf.AsyncPush("myPusher", util.MSG{
			KafkaMsg: util.KafkaMessage{
				ProducerMsg: &sarama.ProducerMessage{
					Topic: "test",
					Value: sarama.ByteEncoder([]byte("ddddddddd" + strconv.Itoa(i))),
				},
			},
		}, func(msg util.MSG, e error) {
			atomic.AddInt32(&count, 1)
			bytes, _ := msg.KafkaMsg.ProducerMsg.Value.Encode()
			fmt.Println(fmt.Sprintf("send async message success:%s or err:%v", string(bytes), e))
		})

		assert.Nil(t, push)
	}

	for {
		if count >= total {
			fmt.Println(count)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func Benchmark_ASyncPush(b *testing.B) {
	//var count *int32

	for i := 0; i < b.N; i++ {
		//producer := kafka.AsyncPusherPool["myPusher"]
		//producer.(*MockAsyncProducer).successChan <- &sarama.ProducerMessage{
		//	Value:    sarama.ByteEncoder([]byte(fmt.Sprintf("send async message：%d", i))),
		//	Metadata: strconv.Itoa(i),
		//}
		_ = kf.AsyncPush("myPusher", util.MSG{
			KafkaMsg: util.KafkaMessage{
				ProducerMsg: &sarama.ProducerMessage{
					Topic: "test",
					Value: sarama.ByteEncoder([]byte("ddddddddd")),
				},
			},
		}, func(msg util.MSG, e error) {
			//atomic.AddInt32(count, 1)
			bytes, _ := msg.KafkaMsg.ProducerMsg.Value.Encode()
			fmt.Println(fmt.Sprintf("send async message success:%s or err:%v", string(bytes), e))
		})
		//assert.Nil(t, push)
	}

}

func TestKafka_Consume(t *testing.T) {
	err := kf.Consume("myPoper", func(msg util.MSG) error {
		fmt.Println("receive Puller message ", string(msg.KafkaMsg.ConsumerMsg.Value), msg.SendTime.Format("2006-01-02 15:04:05"))
		return errors.New("test")
	})

	assert.Nil(t, err)
	time.Sleep(200 * time.Second)
}
