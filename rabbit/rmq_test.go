package rabbit_test

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"go-mq/rabbit"
	"go-mq/util"
	"testing"
	"time"
)

var rb *rabbit.RabbitMQ

func TestMain(t *testing.M) {
	rabbit, err := rabbit.InitRabbitFromPath("../conf/rabbit.yml")
	if err != nil {
		panic(err)
	}
	rb = rabbit
	t.Run()
}

func TestSyncPush(t *testing.T) {
	err := rb.SyncPush("myPusher", util.MSG{
		RabbitMsg: util.RabbitMessage{
			ProducerMsg: &amqp.Publishing{
				Body: []byte("ddddddddd"),
			},
		},
	})
	assert.Nil(t, err)

	err1 := rb.SyncPush("myPusher", util.MSG{
		RabbitMsg: util.RabbitMessage{
			ProducerMsg: &amqp.Publishing{
				Body: []byte("ddddddddd1111"),
			},
		},
	})
	assert.Nil(t, err1)
}

func TestConsume(t *testing.T) {
	err1 := rb.Consume("myPoper", func(msg util.MSG) error {
		fmt.Println(fmt.Sprintf("myPoper收到日志:%s", string(msg.RabbitMsg.ConsumerMsg.Body)))
		return nil
	})

	assert.Nil(t, err1)
	time.Sleep(2 * time.Second)
}

func Test_TxPush(t *testing.T) {
	err := rb.TxPush("myTxPusher", util.MSG{
		RabbitMsg: util.RabbitMessage{
			ProducerMsg: &amqp.Publishing{
				Body: []byte("txxxxxxxxxxxxxxxxxxxxxxxxxfail"),
			},
		},
	}, func() error {
		return errors.New("")
	})
	assert.Nil(t, err)

	_ = rb.TxPush("myTxPusher", util.MSG{
		RabbitMsg: util.RabbitMessage{
			ProducerMsg: &amqp.Publishing{
				Body: []byte("txxxxxxxxxxxxxxxxxxxxxxxxxsuccess"),
			},
		},
	}, func() error {
		return nil
	})
}

func Test_AsyncPush(t *testing.T) {
	receive := make(chan struct{})
	err := rb.AsyncPush("myPusher", util.MSG{
		RabbitMsg: util.RabbitMessage{
			ProducerMsg: &amqp.Publishing{
				Body: []byte("ddddddddd"),
			},
		},
	}, func(msg util.MSG, err error) {
		receive <- struct{}{}
		if err == nil {
			fmt.Printf("成功發送消息:%s ", string(msg.RabbitMsg.ProducerMsg.Body))
		} else {
			fmt.Printf("發送消息失敗:%v", err)
		}
	})
	assert.Nil(t, err)
	<-receive
}

func Test_DelayPush(t *testing.T) {
	_ = rb.DelayPush("myPusher", util.MSG{
		RabbitMsg: util.RabbitMessage{
			ProducerMsg: &amqp.Publishing{
				Body: []byte("delay message"),
			},
		},
	}, 5000)
	receive := make(chan struct{})
	fmt.Println("send delay time : " + time.Now().Format("2006-01-02 15:04:05"))
	err1 := rb.Consume("myPoper", func(msg util.MSG) error {
		fmt.Println(fmt.Sprintf("myPoper收到日志:%s", string(msg.RabbitMsg.ConsumerMsg.Body)))
		receive <- struct{}{}
		return nil
	})
	<-receive
	fmt.Println("receive delay time : " + time.Now().Format("2006-01-02 15:04:05"))
	assert.Nil(t, err1)
}
