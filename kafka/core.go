package kafka

import (
	"github.com/Shopify/sarama"
	"go-mq/util"
)

// KafkaConfig 总配置
type KafkaConfig struct {
	Connects []Connect `json:"Connects" yaml:"Connects"`
	Pushers  []Pusher  `json:"Pushers" yaml:"Pushers"`
	Popers   []Puller  `json:"Pullers" yaml:"Pullers"`

	//单位秒 异步消息时 超过此时间未收到broker响应 则认为失败 默认为10秒
	ConfirmTimeout int64 `json:"ConfirmTimeout" yaml:"ConfirmTimeout"`
}

// Connect 连接信息
type Connect struct {
	Name      string         `json:"Name" yaml:"Name"` // 连接名称
	Addr      string         `json:"Addr" yaml:"Addr"` // 连接地址
	OriginCfg *sarama.Config // kafka原有配置
}

// Pusher 生产者信息
type Pusher struct {
	Name    string `json:"Name" yaml:"Name"`   // 连接名称
	Topic   string `json:"Topic" yaml:"Topic"` // topic
	Connect string `json:"Connect" yaml:"Connect"`
}

// Puller 消费者信息
type Puller struct {
	Name    string `json:"Name" yaml:"Name"`       // 消费者名称
	Topic   string `json:"Topic" yaml:"Topic"`     // topic
	Connect string `json:"Connect" yaml:"Connect"` // 连接名称
	GroupID string `json:"GroupID" yaml:"GroupID"` // 消费组Id
}

var (
	// Cfg 总配置
	Cfg = new(KafkaConfig)
	// SyncPusherPool . 同步发送者
	SyncPusherPool = make(map[string]sarama.SyncProducer)
	// AsyncPusherPool . 异步发送者
	AsyncPusherPool = make(map[string]sarama.AsyncProducer)

	_PusherPool     = make(map[string]Pusher)
	_ConnectPool    = make(map[string]Connect)
	_PoperPool      = make(map[string]Puller)
	_DeliveryMap    = make(map[string]*util.Delivery)
	_ConfirmTimeout int64

	kafkaError = &util.MqError{
		Prefix: "[kafka]",
	}
)
