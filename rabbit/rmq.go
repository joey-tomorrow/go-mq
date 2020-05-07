package rabbit

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"go-mq/util"
	"strconv"
	"sync/atomic"
	"time"
)

//NewRmq 初始化
func NewRmq() *RabbitMQ {
	return &RabbitMQ{}
}

// RabbitMQ .
type RabbitMQ struct{}

// SyncPush 同步发送消息
func (biz *RabbitMQ) SyncPush(pusher string, data util.MSG) error {
	err := validate(data)
	if err != nil {
		return err
	}

	sc := make(chan bool)
	if err := PushCallback(pusher, data, func(msg util.MSG, err error) {
		if err != nil {
			sc <- false
			rabbitError.PrintlnErrorMessage(" PushCallback failed:%v", err)
		} else {
			sc <- true
		}
	}); err != nil {
		rabbitError.PrintlnErrorMessage(" PushCallback failed:%v", err)
		return err
	}

	for {
		select {
		case success := <-sc:
			if !success {
				return fmt.Errorf("[rabbitMq]SyncPush failed")
			}
			return nil
		case <-time.After(200 * time.Millisecond):
			return errors.New("SyncPush failed")
		}
	}
}

//OneWay 单向发送消息
func (biz *RabbitMQ) OneWay(pusher string, data util.MSG) error {
	err := validate(data)
	if err != nil {
		return err
	}

	if err := Push(pusher, data); err != nil {
		rabbitError.PrintlnErrorMessage(" OneWay failed:%v", err)
		return err
	}

	return nil
}

//AsyncPush 异步发送消息
func (biz *RabbitMQ) AsyncPush(pusher string, data util.MSG, callback util.Callback) error {
	err := validate(data)
	if err != nil {
		return err
	}

	if err := PushCallback(pusher, data, callback); err != nil {
		rabbitError.PrintlnErrorMessage(" AsyncPush failed:%v", err)
		return err
	}

	return nil
}

//DelayPush 延迟消息
func (biz *RabbitMQ) DelayPush(pusher string, data util.MSG, delay int64) error {
	err := validate(data)
	if err != nil {
		return err
	}

	if err := DelayPush(pusher, data, delay); err != nil {
		rabbitError.PrintlnErrorMessage(" DelayPush failed:%v", err)
		return err
	}

	return nil
}

//TxPush 事务消息
func (biz *RabbitMQ) TxPush(pusher string, data util.MSG, fn func() error) error {
	err := validate(data)
	if err != nil {
		return err
	}

	if err := TxPush(pusher, data, fn); err != nil {
		rabbitError.PrintlnErrorMessage(" TxPush failed:%v", err)
		return err
	}

	return nil
}

// Consume 消费
func (biz *RabbitMQ) Consume(poper string, callback func(util.MSG) error) error {
	return Pop(poper, callback)
}

//TxPush 事务发消息 保证channel的confirmNowait为true 否则报错
func TxPush(name string, msg util.MSG, fn func() error) (err error) {
	if _, ok := _Pusher[name]; !ok {
		return errors.New("Pusher不存在")
	}

	cfg := _Pusher[name]

	channel, ok := _ChannelPool[cfg.Channel]
	if !ok {
		return errors.New("Channel不存在")
	}

	err = channel.Tx()
	if err != nil {
		rabbitError.PrintlnErrorMessage("开启事务失败【%v】", err)
		return err
	}

	if err = channel.Publish(cfg.Exchange, cfg.Key, cfg.Mandtory, cfg.Immediate,
		*msg.RabbitMsg.ProducerMsg); err != nil {
		return err
	}

	err = fn()
	if err != nil {
		rabbitError.PrintlnErrorMessage("TxPush execute func() failed:%v! Rollback message!", err)
		return channel.TxRollback()
	}

	return channel.TxCommit()
}

//Push 异步向交换机推送一条消息
func Push(name string, msg util.MSG) (err error) {
	if _, ok := _Pusher[name]; !ok {
		return errors.New("Pusher不存在")
	}

	cfg := _Pusher[name]

	if _, ok := _ChannelPool[cfg.Channel]; !ok {
		return errors.New("Channel不存在")
	}

	if err = _ChannelPool[cfg.Channel].Publish(cfg.Exchange, cfg.Key, cfg.Mandtory, cfg.Immediate,
		*msg.RabbitMsg.ProducerMsg); err != nil {
		return err
	}
	return nil
}

//PushCallback 异步向交换机推送一条消息
func PushCallback(name string, msg util.MSG, callback util.Callback) (err error) {
	if _, ok := _Pusher[name]; !ok {
		return errors.New("Pusher不存在")
	}

	cfg := _Pusher[name]

	if _, ok := _ChannelPool[cfg.Channel]; !ok {
		return errors.New("Channel不存在")
	}

	delivery, ok := _DeliveryMap[cfg.Channel]
	if !ok {
		rabbitError.PrintlnErrorMessage("_DeliveryMap not find key:%s", cfg.Channel)
		return fmt.Errorf("_DeliveryMap not find key:%s", cfg.Channel)
	}
	msg.SendTime = time.Now()

	err = Push(name, msg)
	if err != nil {
		rabbitError.PrintlnErrorMessage("Push failed:%v", err)
		return err
	}

	key := strconv.FormatInt(int64(atomic.AddUint64(&delivery.DeliveryTag, 1)), 10)
	delivery.DeliveryMap.Set(key, util.PubMsg{
		Msg:      msg,
		Callback: callback,
	})

	return nil
}

func doConfirm(v *util.Delivery, key string, ack bool) {
	pm, ok := v.DeliveryMap.Pop(key)
	if !ok {
		return
	}

	publishMsg, ok := pm.(util.PubMsg)
	if !ok {
		rabbitError.PrintlnErrorMessage("publishMsg:%v is not util.PubMsg type", pm)
		return
	}

	if ack {
		publishMsg.Callback(publishMsg.Msg, nil)
	} else {
		publishMsg.Callback(publishMsg.Msg, util.ErrMsgNotConfirmed)
	}

	v.DeliveryMap.Remove(key)
}

func doTimeout(v *util.Delivery) {
	for m := range v.DeliveryMap.IterBuffered() {
		// 超时未响应
		pm, ok := m.Val.(util.PubMsg)
		if !ok {
			rabbitError.PrintlnErrorMessage("publishMsg:%v is not util.PubMsg type", pm)
			return
		}
		if pm.Msg.SendTime.Add(30 * time.Second).Before(time.Now()) {
			if pm.Callback != nil {
				go func(pubMsg util.PubMsg) {
					defer func() {
						if e := recover(); e != nil {
							rabbitError.PrintlnErrorMessage("doTimeOut callback failed %v", e)
						}
					}()

					pubMsg.Callback(pubMsg.Msg, util.ErrMsgTimeout)
				}(pm)
			}
			v.DeliveryMap.Remove(m.Key)
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
						rabbitError.PrintlnErrorMessage("publishMsg:%v is not util.PubMsg type", publishMsg)
						continue
					}

					msg.Callback(msg.Msg, util.ErrShutdownHook)
				}
			}()

			for {
				select {
				case confirm := <-_ChannelConfirm[v.Channel]:
					key := strconv.FormatInt(int64(confirm.DeliveryTag), 10)
					doConfirm(v, key, confirm.Ack)
				case <-ticker.C:
					doTimeout(v)
				}
			}
		}(v)
	}
}

//SyncPush 同步向交换机推送一条消息
func SyncPush(name string, msg util.MSG) (err error) {
	err = Push(name, msg)
	if err != nil {
		rabbitError.PrintlnErrorMessage("Push failed:%v", err)
		return err
	}

	cfg := _Pusher[name]
	if _, ok := _ChannelPool[cfg.Channel]; !ok {
		return errors.New("Channel不存在")
	}

	// 默认500MS超时
	ticker := time.NewTicker(500 * time.Millisecond)
LOOP:
	for {
		select {
		case <-_ChannelReturn[cfg.Channel]:
			fmt.Println("Push didn't confirm.")
			return util.ErrMsgNotConfirmed
		case <-ticker.C:
			break LOOP
		}
	}

	return nil
}

// DelayPush 延时消息
func DelayPush(name string, msg util.MSG, delayTime int64) (err error) {
	if _, ok := _Pusher[name]; !ok {
		return errors.New("Pusher不存在")
	}

	cfg := _Pusher[name]

	if _, ok := _ChannelPool[cfg.Channel]; !ok {
		return errors.New("Channel不存在")
	}

	if msg.RabbitMsg.ProducerMsg.Headers == nil {
		msg.RabbitMsg.ProducerMsg.Headers = amqp.Table{
			"x-delay": delayTime,
		}
	} else {
		msg.RabbitMsg.ProducerMsg.Headers["x-delay"] = delayTime
	}

	if err = _ChannelPool[cfg.Channel].Publish(getDelayExgName(cfg.Exchange), cfg.Key, cfg.Mandtory, cfg.Immediate,
		*msg.RabbitMsg.ProducerMsg); err != nil {
		return err
	}
	return nil
}

// DelayAsyncPush 延时消息
func DelayAsyncPush(name string, key string, msg util.MSG, delayTime int, callback util.Callback) (err error) {
	if _, ok := _Pusher[name]; !ok {
		return errors.New("Pusher不存在")
	}

	cfg := _Pusher[name]
	if key != "" {
		cfg.Key = key
	}
	if _, ok := _ChannelPool[cfg.Channel]; !ok {
		return errors.New("Channel不存在")
	}

	delivery, ok := _DeliveryMap[cfg.Channel]
	if !ok {
		rabbitError.PrintlnErrorMessage("_DeliveryMap not find key:%s", cfg.Channel)
		return fmt.Errorf("_DeliveryMap not find key:%s", cfg.Channel)
	}
	msg.SendTime = time.Now()

	msg.RabbitMsg.ProducerMsg.Headers["x-delay"] = delayTime
	if err = _ChannelPool[cfg.Channel].Publish(getDelayExgName(cfg.Exchange), cfg.Key, cfg.Mandtory, cfg.Immediate,
		*msg.RabbitMsg.ProducerMsg); err != nil {
		return err
	}

	key = strconv.FormatInt(int64(atomic.AddUint64(&delivery.DeliveryTag, 1)), 10)
	delivery.DeliveryMap.Set(key, util.PubMsg{
		Msg:      msg,
		Callback: callback,
	})

	return nil
}

//处理消息(顺序处理,如果需要多线程可以在回调函数中做手脚)
func handleMsg(msgs <-chan amqp.Delivery, callback func(msg util.MSG) error, channel string, poperName, QName string, waitChan chan struct{}) {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				rabbitError.PrintlnErrorMessage("handleMsg panic error %v", e)
			}
		}()

		defer func() {
			waitChan <- struct{}{}
		}()

		for m := range msgs {
			msg := m
			rabbitError.PrintlnErrorMessage("receive msg %s", string(msg.Body))
			err := callback(util.MSG{
				RabbitMsg: util.RabbitMessage{
					ConsumerMsg: &msg,
				},
			})

			level := msg.Headers["level"]
			if err == nil {
				_ = msg.Ack(false)
				continue
			}

			// 消息等级为可丢弃，则直接ack放弃重试
			if level == util.MSGLevelDiscard {
				_ = msg.Ack(false)
				continue
			}

			// 重试次数大于默认最大重试次数则放入error队列中
			retry := 0
			if _QueueMap[QName].Retry == nil {
				retry = DefaultMessageRetryTimes
			} else {
				retry = _QueueMap[QName].Retry.Times
			}

			if CurrentMessageRetries(msg) >= retry {
				//go send2Error(msg)
				_ = msg.Ack(false)
				continue
			} else {
				// 消息等级为不可丢失，则重新丢回队列
				_ = msg.Reject(false)
			}
		}
	}()
}

//Pop 从队列获取消息 -- 推模式
func Pop(name string, callback func(msg util.MSG) error) (err error) {
	if _, ok := _Poper[name]; !ok {
		return errors.New("Poper不存在")
	}
	cfg := _Poper[name]
	if _, ok := _ChannelPool[cfg.Channel]; !ok {
		return errors.New("Channel不存在")
	}

	go func() {
		defer func() {
			if e := recover(); e != nil {
				rabbitError.PrintlnErrorMessage("Pop panic error %v", e)
			}
		}()

		for {
			var msgs <-chan amqp.Delivery
			if msgs, err = _ChannelPool[cfg.Channel].Consume(cfg.QName, cfg.Consumer,
				cfg.AutoACK, cfg.Exclusive, cfg.NoLocal, cfg.NoWait, nil); err != nil {
				rabbitError.PrintlnErrorMessage("Pop to Consume message failed:%v", err)

				time.Sleep(DefaultRetryInterval * time.Second)
				continue
			}

			var waitChan = make(chan struct{})
			handleMsg(msgs, callback, cfg.Channel, name, cfg.QName, waitChan)
			<-waitChan

			rabbitError.PrintlnErrorMessage("Retrying pop name:%s ... ...", name)
		}
	}()

	return nil
}

func validate(msg util.MSG) error {
	if msg.RabbitMsg.ProducerMsg == nil {
		rabbitError.PrintlnErrorMessage("RabbitMsg>ProducerMsg can not be null")
		return util.ErrMsgEmpty
	}
	return nil
}

//DoPop .
func DoPop(name string, callback func(msg util.MSG) error, wait chan struct{}) (err error) {
	if _, ok := _Poper[name]; !ok {
		return errors.New("Poper不存在")
	}
	cfg := _Poper[name]
	if _, ok := _ChannelPool[cfg.Channel]; !ok {
		return errors.New("Channel不存在")
	}

	var msgs <-chan amqp.Delivery
	if msgs, err = _ChannelPool[cfg.Channel].Consume(cfg.QName, cfg.Consumer,
		cfg.AutoACK, cfg.Exclusive, cfg.NoLocal, cfg.NoWait, nil); err != nil {
		return err
	}

	handleMsg(msgs, callback, cfg.Channel, name, cfg.QName, wait)
	return nil
}

// CurrentMessageRetries 获取消息的重试次数
func CurrentMessageRetries(msg amqp.Delivery) int {
	xDeathArray, ok := msg.Headers["x-death"].([]interface{})
	if !ok {
		fmt.Println("x-death array case fail")
		return 0
	}

	if len(xDeathArray) <= 0 {
		return 0
	}

	for _, h := range xDeathArray {
		xDeathItem, ok := h.(amqp.Table)
		if !ok {
			fmt.Println("x-death type is notamqp.Table")
			return 0
		}
		if xDeathItem["reason"] == "rejected" {
			return int(xDeathItem["count"].(int64))
		}
	}

	return 0
}

// CloneToPublishMsg 克隆消息
func CloneToPublishMsg(msg *amqp.Delivery) *amqp.Publishing {
	newMsg := amqp.Publishing{
		Headers: msg.Headers,

		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,

		Body: msg.Body,
	}

	return &newMsg
}
