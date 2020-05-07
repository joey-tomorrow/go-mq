package util

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/streadway/amqp"
	"hidevops.io/hiboot/pkg/utils/cmap"
	"time"
)

// Callback .
type Callback func(MSG, error)

// MSG .
type MSG struct {
	//Level 消息等级
	Level MSGLevel `json:"level"`
	//SendTime 消息发送时间
	SendTime time.Time

	//KafkaMsg kafka的消息，包括发送的消息体、接收的消息体
	KafkaMsg KafkaMessage
	//RabbitMsg rabbitMq的消息，包括发送的消息体、接收的消息体
	RabbitMsg RabbitMessage
}

//kafka消息
type KafkaMessage struct {
	//发送的消息
	ProducerMsg *sarama.ProducerMessage
	//接收的消息
	ConsumerMsg *sarama.ConsumerMessage
}

//rabbit消息
type RabbitMessage struct {
	//发送的消息
	ProducerMsg *amqp.Publishing
	//接收的消息
	ConsumerMsg *amqp.Delivery
}

//Delivery .
type Delivery struct {
	Channel     string
	DeliveryTag uint64
	DeliveryMap cmap.ConcurrentMap
}

// PubMsg .
type PubMsg struct {
	Msg      MSG
	Callback Callback
}

type MqError struct {
	Prefix string
}

func NewMqError(prefix string) *MqError {
	return &MqError{
		Prefix: prefix,
	}
}

func (m *MqError) PrintlnErrorMessage(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(m.Prefix+format, a...))
}
