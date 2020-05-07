package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"go-mq/util"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

//NewKafka .
func NewKafka() *Kafka {
	return &Kafka{}
}

// Kafka .
type Kafka struct{}

// SyncPush 同步发送
func (biz *Kafka) SyncPush(pusher string, data util.MSG) error {
	err := validate(data)
	if err != nil {
		return err
	}

	producer, ok := SyncPusherPool[pusher]
	if !ok {
		return fmt.Errorf("[Kafka]pusher:%s not found", pusher)
	}

	if data.KafkaMsg.ProducerMsg == nil {
		return fmt.Errorf("[Kafka] KafkaMsg.ProducerMsg can not be null")
	}

	data.SendTime = time.Now()

	partition, offset, err := producer.SendMessage(data.KafkaMsg.ProducerMsg)
	if err != nil {
		return err
	}

	kafkaError.PrintlnErrorMessage("SyncPush success [partition:%d offset:%d]", partition, offset)
	return nil
}

// OneWay .
// 并不是严格意义上的单向发送
// 严格意义的单向发送 需要设置ack==0
// 这里其实是异步发送后对confirm信息不做处理而已
func (biz *Kafka) OneWay(pusher string, data util.MSG) error {
	err := validate(data)
	if err != nil {
		return err
	}

	return biz.AsyncPush(pusher, data, nil)
}

// AsyncPush .
func (biz *Kafka) AsyncPush(pusher string, data util.MSG, callback util.Callback) error {
	err := validate(data)
	if err != nil {
		return err
	}

	producer, ok := AsyncPusherPool[pusher]
	if !ok {
		return fmt.Errorf("[Kafka]pusher:%s not found", pusher)
	}

	delivery, ok := _DeliveryMap[pusher]
	if !ok {
		kafkaError.PrintlnErrorMessage("_DeliveryMap not find sequence:%s", pusher)
		return fmt.Errorf("[Kafka]_DeliveryMap not find sequence:%s", pusher)
	}

	sequence := strconv.FormatInt(int64(atomic.AddUint64(&delivery.DeliveryTag, 1)), 10)
	data.SendTime = time.Now()

	// 设置消息等级&序列号
	data.KafkaMsg.ProducerMsg.Headers = append(data.KafkaMsg.ProducerMsg.Headers, sarama.RecordHeader{
		Key:   []byte(util.MSGLevelKey),
		Value: []byte(string(data.Level)),
	}, sarama.RecordHeader{
		Key:   []byte(util.PushSequenceKey),
		Value: []byte(sequence),
	})

	producer.Input() <- data.KafkaMsg.ProducerMsg

	delivery.DeliveryMap.Set(sequence, util.PubMsg{
		Msg:      data,
		Callback: callback,
	})
	return nil
}

func doConfirm(delivery *util.Delivery, key string) {
	pm, ok := delivery.DeliveryMap.Pop(key)
	if !ok {
		return
	}

	publishMsg, ok := pm.(util.PubMsg)
	if !ok {
		kafkaError.PrintlnErrorMessage("publishMsg:%v is not util.PubMsg type", pm)
		return
	}
	if publishMsg.Callback != nil {
		publishMsg.Callback(publishMsg.Msg, nil)
	}
	delivery.DeliveryMap.Remove(key)
}

func doFail(delivery *util.Delivery, key string, err error) {
	pm, ok := delivery.DeliveryMap.Pop(key)
	if !ok {
		return
	}

	publishMsg, ok := pm.(util.PubMsg)
	if !ok {
		kafkaError.PrintlnErrorMessage("publishMsg:%v is not util.PubMsg type", pm)
		return
	}
	if publishMsg.Callback != nil {
		publishMsg.Callback(publishMsg.Msg, err)
	}
	delivery.DeliveryMap.Remove(key)
}

func doTimeOut(delivery *util.Delivery) {
	for m := range delivery.DeliveryMap.IterBuffered() {
		// 超时未响应
		pm, ok := m.Val.(util.PubMsg)
		if !ok {
			kafkaError.PrintlnErrorMessage("publishMsg:%v is not util.PubMsg type", pm)
			continue
		}

		if pm.Msg.SendTime.Add(time.Duration(_ConfirmTimeout) * time.Second).Before(time.Now()) {
			if pm.Callback != nil {
				go func(pubMsg util.PubMsg) {
					defer func() {
						if e := recover(); e != nil {
							kafkaError.PrintlnErrorMessage("doTimeOut callback failed %v", e)
						}
					}()

					pubMsg.Callback(pubMsg.Msg, util.ErrMsgTimeout)
				}(pm)
			}
			delivery.DeliveryMap.Remove(m.Key)
		}
	}
}

func callbackProducer() {
	for _, v := range _DeliveryMap {
		go func(v *util.Delivery) {
			ticker := time.NewTicker(1 * time.Second)
			defer func() {
				ticker.Stop()
				for publishMsg := range v.DeliveryMap.IterBuffered() {
					msg, ok := publishMsg.Val.(util.PubMsg)
					if !ok {
						kafkaError.PrintlnErrorMessage("publishMsg:%v is not util.PubMsg type", msg)
						continue
					}

					if msg.Callback != nil {
						msg.Callback(msg.Msg, util.ErrShutdownHook)
					}
				}
			}()

			for {
				select {
				case confirm := <-AsyncPusherPool[v.Channel].Successes():
					key := findSequence(confirm.Headers)
					doConfirm(v, key)

				case fail := <-AsyncPusherPool[v.Channel].Errors():
					key := findSequence(fail.Msg.Headers)
					doFail(v, key, fail.Err)

				case <-ticker.C:
					doTimeOut(v)
				}
			}
		}(v)
	}
}

func findSequence(headers []sarama.RecordHeader) string {
	for _, v := range headers {
		if string(v.Key) == util.PushSequenceKey {
			return string(v.Value)
		}
	}
	return ""
}

// DelayPush . kafka的延时消息暂不支持
func (biz *Kafka) DelayPush(pusher string, data util.MSG, delay int64) error {
	return util.ErrMethodNotSupport
}

//TxPush . kafka的事务消息暂不支持
func (biz *Kafka) TxPush(pusher string, data util.MSG, fn func() error) error {
	return util.ErrMethodNotSupport
}

// Consume . 调用一次
func (biz *Kafka) Consume(poperName string, callback func(util.MSG) error) error {
	poper, ok := _PoperPool[poperName]
	if !ok {
		return fmt.Errorf("[Kafka]poper:%s not found", poperName)
	}

	connect, ok := _ConnectPool[poper.Connect]
	if !ok {
		return fmt.Errorf("[Kafka]connect:%s not found", poper.Connect)
	}

	go func() {
		defer func() {
			if e := recover(); e != nil {
				kafkaError.PrintlnErrorMessage("Pop panic error %v", e)
			}
		}()

		for {
			var waitChan = make(chan struct{})
			go func() {
				defer func() {
					if err := recover(); err != nil {
						kafkaError.PrintlnErrorMessage("go func failed:%v", err)
					}
					waitChan <- struct{}{}
				}()

				connect.OriginCfg.Version = sarama.V0_11_0_2
				group, err := sarama.NewConsumerGroup(strings.Split(connect.Addr, ","), poper.GroupID, connect.OriginCfg)
				if err != nil {
					kafkaError.PrintlnErrorMessage("NewConsumerGroup failed:%v", err)
					return
				}

				handler := commonConsumerGroupHandler{
					callback: callback,
				}
				err = group.Consume(context.Background(), []string{poper.Topic}, handler)
				if err != nil {
					kafkaError.PrintlnErrorMessage("Consume failed:%v", err)
					return
				}

			}()

			<-waitChan
			kafkaError.PrintlnErrorMessage("Consume Retry connect ......")
			time.Sleep(1 * time.Second)
		}
	}()

	return nil
}

type commonConsumerGroupHandler struct {
	callback func(util.MSG) error
}

func (commonConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (commonConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h commonConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		kafkaError.PrintlnErrorMessage("Receive Message[%s] topic:%q partition:%d offset:%d", string(msg.Value), msg.Topic, msg.Partition, msg.Offset)
		mm := &util.MSG{
			KafkaMsg: util.KafkaMessage{
				ConsumerMsg: msg,
			},
		}

		var level string
		for _, v := range msg.Headers {
			if util.MSGLevelKey == string(v.Key) {
				level = string(v.Value)
			}
		}

		err := h.callback(*mm)
		if err == nil || level == string(util.MSGLevelDiscard) {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}

func validate(msg util.MSG) error {
	if msg.KafkaMsg.ProducerMsg == nil {
		kafkaError.PrintlnErrorMessage("KafkaMsg>ProducerMsg can not be null")
		return util.ErrMsgEmpty
	}
	return nil
}
