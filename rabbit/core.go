package rabbit

import (
	"github.com/streadway/amqp"
	"go-mq/util"
)

const (
	// DefaultRetryInterval 断线重连默认间隔时间 秒
	DefaultRetryInterval = 3
	// DefaultMessageRetryTimes 消息重试次数
	DefaultMessageRetryTimes = 3
	// DefaultMessageRetryInterval 消息重试间隔时间 秒
	DefaultMessageRetryInterval = 5 * 60
)

//Cfg .
var (
	Cfg                = new(RabbitMQConfig)
	_ConnectPool       = make(map[string]*amqp.Connection) //连接名称:连接对象
	_ChannelPool       = make(map[string]*amqp.Channel)    //信道名称:信道对象
	_ExchangePool      = make(map[string]string)           //交换机名称:所属信道名称
	_QueuePool         = make(map[string]string)           //队列名称:所属信道名称
	_Pusher            = make(map[string]Pusher)           //Pusher名称:Pusher配置
	_ConnectProperties = make(map[string]Connect)          //connect配置
	_ChannelProperties = make(map[string]Channel)          //connect配置
	_Poper             = make(map[string]Puller)           //Poper名称:Poper配置
	_ReconnectLock     = make(chan struct{}, 1)            //断线重连时加锁

	_QueueMap       = make(map[string]Queue)                  // 队列map
	_ChannelConfirm = make(map[string]chan amqp.Confirmation) // 每个channel对应的confirm通知
	_ChannelReturn  = make(map[string]chan amqp.Return)       // 每个channel对应的confirm通知
	_DeliveryMap    = make(map[string]*util.Delivery)

	rabbitError = &util.MqError{
		Prefix: "[RabbitMQ]",
	}
)

//Connect 连接结构
type Connect struct {
	Name     string `json:"Name" yaml:"Name"`
	Addr     string `json:"Addr" yaml:"Addr"`
	Delay    bool   `json:"Delay" yaml:"Delay"` // 是否支持延时队列（必须安装delayed_message_exchange插件）
	Interval int    `json:"Interval" yaml:"Interval"`
}

//Channel 信道结构
type Channel struct {
	Name          string `json:"Name" yaml:"Name"`
	Connect       string `json:"Connect" yaml:"Connect"`
	QosCount      int    `json:"QosCount" yaml:"QosCount"`
	QosSize       int    `json:"QosSize" yaml:"QosSize"`
	ConfirmNoWait bool   `json:"Confirm" yaml:"ConfirmNoWait"`
}

//EBind 交换机绑定结构
type EBind struct {
	Destination string `json:"Destination" yaml:"Destination"`
	Key         string `json:"Key" yaml:"Key"`
	NoWait      bool   `json:"NoWait" yaml:"NoWait"`
}

//Exchange 交换机结构
type Exchange struct {
	Name        string                 `json:"Name" yaml:"Name"`
	Channel     string                 `json:"Channel" yaml:"Channel"`
	Type        string                 `json:"Type" yaml:"Type"`
	Durable     bool                   `json:"Durable" yaml:"Durable"`
	AutoDeleted bool                   `json:"AutoDeleted" yaml:"AutoDeleted"`
	Internal    bool                   `json:"Internal" yaml:"Internal"`
	NoWait      bool                   `json:"NoWait" yaml:"NoWait"`
	Bind        []EBind                `json:"Bind" yaml:"Bind"`
	Args        map[string]interface{} `json:"Args" yaml:"Args"`
}

//QBind 队列绑定结构
type QBind struct {
	ExchangeName string `json:"ExchangeName" yaml:"ExchangeName"`
	Key          string `json:"Key" yaml:"Key"`
	NoWait       bool   `json:"NoWait" yaml:"NoWait"`
}

//QueueRetry 重试队列
type QueueRetry struct {
	Times    int `json:"Times" yaml:"Times"`
	Interval int `json:"Interval" yaml:"Interval"` // 重试间隔 * 秒
}

//Queue 队列结构
type Queue struct {
	Name       string                 `json:"Name" yaml:"Name"`
	Channel    string                 `json:"Channel" yaml:"Channel"`
	Durable    bool                   `json:"Durable" yaml:"Durable"`
	AutoDelete bool                   `json:"AutoDelete" yaml:"AutoDelete"`
	Exclusive  bool                   `json:"Exclusive" yaml:"Exclusive"`
	NoWait     bool                   `json:"NoWait" yaml:"NoWait"`
	Bind       []QBind                `json:"Bind" yaml:"Bind"`
	Delay      []int                  `json:"Delay" yaml:"Delay"`
	Args       map[string]interface{} `json:"Args" yaml:"Args"`
	Retry      *QueueRetry            `json:"Retry" yaml:"Retry"`
}

//Pusher 发送者配置
type Pusher struct {
	Name         string `json:"Name" yaml:"Name"`
	Channel      string `json:"Channel" yaml:"Channel"`
	Exchange     string `json:"Exchange" yaml:"Exchange"`
	Key          string `json:"Key" yaml:"Key"`
	Mandtory     bool   `json:"Mandtory" yaml:"Mandtory"`
	Immediate    bool   `json:"Immediate" yaml:"Immediate"`
	ContentType  string `json:"ContentType" yaml:"ContentType"`
	DeliveryMode uint8  `json:"DeliveryMode" yaml:"DeliveryMode"`
}

//Puller 接收者配置
type Puller struct {
	Name      string `json:"Name" yaml:"Name"`
	QName     string `json:"QName" yaml:"QName"`
	Channel   string `json:"Channel" yaml:"Channel"`
	Consumer  string `json:"Consumer" yaml:"Consumer"`
	AutoACK   bool   `json:"AutoACK" yaml:"AutoACK"`
	Exclusive bool   `json:"Exclusive" yaml:"Exclusive"`
	NoLocal   bool   `json:"NoLocal" yaml:"NoLocal"`
	NoWait    bool   `json:"NoWait" yaml:"NoWait"`
}

//RabbitMQConfig 配置文件结构
type RabbitMQConfig struct {
	Connects  []Connect  `json:"Connects" yaml:"Connects"`
	Channels  []Channel  `json:"Channels" yaml:"Channels"`
	Exchanges []Exchange `json:"Exchanges" yaml:"Exchanges"`
	Queue     []Queue    `json:"Queue" yaml:"Queue"`
	Pusher    []Pusher   `json:"Pusher" yaml:"Pusher"`
	Puller    []Puller   `json:"Puller" yaml:"Puller"`
}
