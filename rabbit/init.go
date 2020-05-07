package rabbit

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"go-mq/util"
	"gopkg.in/yaml.v2"
	"hidevops.io/hiboot/pkg/utils/cmap"
	"io/ioutil"
	"os"
	"time"
)

//InitRabbitFromPath 从文件路径初始化
func InitRabbitFromPath(path string) (*RabbitMQ, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}

	return InitRabbit(cfg)
}

//LoadConfig 读取配置文件
func LoadConfig(path string) (cfg *RabbitMQConfig, err error) {
	fp, err := os.Open(path)
	if err != nil {
		rabbitError.PrintlnErrorMessage("LoadConfig from path:%s failed:%v", path, err)
		return nil, err
	}
	defer fp.Close()

	cfg = &RabbitMQConfig{}

	var data []byte
	if data, err = ioutil.ReadAll(fp); err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return
}

//InitRabbit 初始化
func InitRabbit(cfg *RabbitMQConfig) (rm *RabbitMQ, err error) {
	Cfg = cfg
	if err = initConnect(); err != nil {
		return
	}
	if err = initChannel(); err != nil {
		return
	}
	if err = initExchange(); err != nil {
		return
	}
	if err = initQueue(); err != nil {
		return
	}
	if err = initPusher(); err != nil {
		return
	}
	if err = initPoper(); err != nil {
		return
	}

	callbackProducer()
	_ReconnectLock <- struct{}{}
	rm = &RabbitMQ{}
	return
}

//initConnect 初始化连接
func initConnect() (err error) {
	for _, v := range Cfg.Connects {
		if err = HandleConnect(v); err != nil {
			return err
		}
	}
	return nil
}

//initChannel 初始化信道
func initChannel() (err error) {
	for _, v := range Cfg.Channels {
		ready := make(chan struct{})
		HandleChannel(v, ready)
		_ChannelProperties[v.Name] = v
		_DeliveryMap[v.Name] = &util.Delivery{
			Channel:     v.Name,
			DeliveryTag: 0,
			DeliveryMap: cmap.New(),
		}
		<-ready
	}
	return nil
}

//initExchange 初始化交换机
func initExchange() (err error) {
	for _, v := range Cfg.Exchanges {
		if err = CreateExchange(v); err != nil {
			return err
		}
	}
	return nil
}

//初始化队列
func initQueue() (err error) {
	for _, v := range Cfg.Queue {
		if err = CreateQueue(v); err != nil {
			return err
		}
		_QueueMap[v.Name] = v
	}
	return nil
}

//initPusher 初始化Pusher
func initPusher() (err error) {
	for _, v := range Cfg.Pusher {
		if err = CreatePusher(v); err != nil {
			return err
		}
	}
	return err
}

//initPoper 初始化Poper
func initPoper() (err error) {
	for _, v := range Cfg.Puller {
		if err = CreatePoper(v); err != nil {
			return err
		}
	}
	return nil
}

//CloseConnect 关闭连接
func CloseConnect(name string) (err error) {
	if _, ok := _ConnectPool[name]; ok {
		_ConnectPool[name].Close()
	}
	return nil
}

//HandleConnect 重连
func HandleConnect(v Connect) (err error) {
	for CreateConnect(v) != nil {
		interval := DefaultRetryInterval
		if v.Interval > 0 {
			interval = v.Interval
		}

		time.Sleep(time.Duration(interval) * time.Second)
		rabbitError.PrintlnErrorMessage("connect to %s failed, Retry ...", v.Addr)
	}

	_ConnectProperties[v.Name] = v
	return nil
}

//CreateConnect 创建连接
func CreateConnect(v Connect) (err error) {
	var connect *amqp.Connection
	if connect, err = amqp.Dial(v.Addr); err != nil {
		return err
	}
	_ConnectPool[v.Name] = connect
	return nil
}

//CloseChannel 关闭信道
func CloseChannel(name string) (err error) {
	if _, ok := _ChannelPool[name]; ok {
		_ChannelPool[name].Close()
	}
	return nil
}

//HandleChannel .
func HandleChannel(v Channel, ready chan struct{}) {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				rabbitError.PrintlnErrorMessage("HandleConnect panic error %v", e)
			}
		}()

		for {
			// 注册通道关闭
			notifyClose := make(chan *amqp.Error)
			err := CreateChannel(v, notifyClose)
			if err != nil {
				rabbitError.PrintlnErrorMessage("CreateChannel failed:%v!", err)
				break
			}

			ready <- struct{}{}

		loop:
			for {
				select {
				case mqErr := <-notifyClose:
					rabbitError.PrintlnErrorMessage("receive notifyClose err:%v!", mqErr)
					break loop
				}
			}
		}
	}()
}

//CreateChannel 创建信道
func CreateChannel(v Channel, notifyClose chan *amqp.Error) (err error) {
	if _, ok := _ConnectPool[v.Connect]; !ok {
		return errors.New("连接不存在")
	}

	// 双重检查
	if _ConnectPool[v.Connect].IsClosed() {
		<-_ReconnectLock
		if _ConnectPool[v.Connect].IsClosed() {
			for _, c := range Cfg.Connects {
				if c.Name == v.Connect {
					_ = HandleConnect(c)
				}
			}
		}
		_ReconnectLock <- struct{}{}
	}

	var channel *amqp.Channel
	if channel, err = _ConnectPool[v.Connect].Channel(); err != nil {
		return err
	}

	_ChannelPool[v.Name] = channel

	if err = channel.Qos(v.QosCount, v.QosSize, false); err != nil {
		return err
	}

	if !v.ConfirmNoWait {
		err = channel.Confirm(v.ConfirmNoWait)
		if err != nil {
			rabbitError.PrintlnErrorMessage("set confirm failed:%s", err)
			return err
		}
		confirmation := make(chan amqp.Confirmation)
		_ChannelConfirm[v.Name] = confirmation
		channel.NotifyPublish(confirmation)
	}

	notifyReturn := make(chan amqp.Return)
	_ChannelReturn[v.Name] = notifyReturn
	channel.NotifyReturn(notifyReturn)
	channel.NotifyClose(notifyClose)
	return nil
}

//DeleteExchange 删除交换机
func DeleteExchange(name string, ifUnused bool, noWait bool) (err error) {
	if _, ok := _ExchangePool[name]; ok {
		if _, ok := _ChannelPool[_ExchangePool[name]]; ok {
			if err = _ChannelPool[_ExchangePool[name]].ExchangeDelete(
				name, ifUnused, noWait); err != nil {
				return err
			}
		}
		delete(_ExchangePool, name)
	}
	return nil
}

//CreateExchange 创建交换机
func CreateExchange(v Exchange) (err error) {
	if _, ok := _ChannelPool[v.Channel]; !ok {
		return errors.New("信道不存在")
	}

	if err = _ChannelPool[v.Channel].ExchangeDeclare(v.Name, v.Type,
		v.Durable, v.AutoDeleted, v.Internal, v.NoWait, v.Args); err != nil {
		return err
	}

	_ExchangePool[v.Name] = v.Channel

	// 判断是否支持延时队列
	if channel, ok := _ChannelProperties[v.Channel]; ok {
		if _ConnectProperties[channel.Connect].Delay {
			if err = _ChannelPool[v.Channel].ExchangeDeclare(getDelayExgName(v.Name), "x-delayed-message",
				v.Durable, v.AutoDeleted, v.Internal, v.NoWait, map[string]interface{}{"x-delayed-type": "direct"}); err != nil {
				return err
			}
			_ExchangePool[getDelayExgName(v.Name)] = v.Channel
		}
	}

	for _, b := range v.Bind {
		if err = _ChannelPool[v.Channel].ExchangeBind(b.Destination, b.Key, v.Name, b.NoWait, nil); err != nil {
			return err
		}
	}
	return nil
}

//DeleteQueue 删除队列
func DeleteQueue(name string, ifUnused bool, ifEmpty bool, noWait bool) (err error) {
	if _, ok := _QueuePool[name]; ok {
		if _, ok := _ChannelPool[_QueuePool[name]]; ok {
			if _, err = _ChannelPool[_QueuePool[name]].QueueDelete(
				name, ifUnused, ifEmpty, noWait); err != nil {
				return err
			}
		}
		delete(_QueuePool, name)
	}
	return nil
}

//CreateQueue 创建队列
func CreateQueue(v Queue) (err error) {
	if _, ok := _ChannelPool[v.Channel]; !ok {
		return errors.New("信道不存在")
	}

	// 配置Retry 则默认生成重试队列
	if v.Retry != nil {
		if err := createRetryQueue(v); err != nil {
			rabbitError.PrintlnErrorMessage("创建重试队列失败[%v]", err)
			return err
		}
		return nil
	}

	err = createBaseQueue(v)
	if err != nil {
		rabbitError.PrintlnErrorMessage("createBaseQueue error:%s", err)
		return err
	}

	return nil
}

func createBaseQueue(v Queue) (err error) {
	if _, err = _ChannelPool[v.Channel].QueueDeclare(v.Name, v.Durable,
		v.AutoDelete, v.Exclusive, v.NoWait, v.Args); err != nil {
		return err
	}
	_QueuePool[v.Name] = v.Channel

	for _, b := range v.Bind {
		if err = _ChannelPool[v.Channel].QueueBind(v.Name, b.Key, b.ExchangeName, b.NoWait, nil); err != nil {
			return err
		}

		if err = _ChannelPool[v.Channel].QueueBind(v.Name, b.Key, getDelayExgName(b.ExchangeName), false, nil); err != nil {
			rabbitError.PrintlnErrorMessage("绑定[%s]延迟队列失败", v.Name)
			return err
		}
	}

	return nil
}

func createRetryQueue(v Queue) error {
	err := prepareDlxExchange(v)
	if err != nil {
		rabbitError.PrintlnErrorMessage("prepareDlxExchange failed:%v", err)
		return err
	}

	// 定义重试队列
	rabbitError.PrintlnErrorMessage("declaring retry queue: %s\n", v.Name)
	if v.Retry.Interval == 0 {
		v.Retry.Interval = DefaultMessageRetryInterval
	}

	retryQueueOptions := map[string]interface{}{
		"x-dead-letter-exchange": getRequeueExgName(v.Name),
		"x-message-ttl":          int32(v.Retry.Interval * 1000),
	}

	if _, err := _ChannelPool[v.Channel].QueueDeclare(getRetryQueueName(v.Name), v.Durable,
		v.AutoDelete, v.Exclusive, v.NoWait, retryQueueOptions); err != nil {
		rabbitError.PrintlnErrorMessage("定义[%s]重试队列失败", v.Name)
		return err
	}

	if err = _ChannelPool[v.Channel].QueueBind(getRetryQueueName(v.Name), "#", getRetryExgName(v.Name), false, nil); err != nil {
		rabbitError.PrintlnErrorMessage("绑定[%s]重试队列失败", v.Name)
		return err
	}

	if v.Args != nil {
		v.Args["x-dead-letter-exchange"] = getRetryExgName(v.Name)
	} else {
		v.Args = map[string]interface{}{
			"x-dead-letter-exchange": getRetryExgName(v.Name),
		}
	}

	err = createBaseQueue(v)
	if err != nil {
		rabbitError.PrintlnErrorMessage("createBaseQueue error:%s", err)
		return err
	}

	if err = _ChannelPool[v.Channel].QueueBind(v.Name, "#", getRequeueExgName(v.Name), false, nil); err != nil {
		rabbitError.PrintlnErrorMessage("bind work queue to requeue exchange error:%s", err)
		return err
	}

	return nil
}

func prepareDlxExchange(v Queue) error {
	exchanges := []string{
		getRetryExgName(v.Name),
		getRequeueExgName(v.Name),
	}

	for _, e := range exchanges {
		rabbitError.PrintlnErrorMessage("declaring exchange: %s\n", e)

		err := _ChannelPool[v.Channel].ExchangeDeclare(e, "topic", true, false, false, false, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

// 重新入队Exg
func getRequeueExgName(name string) string {
	return fmt.Sprintf("requeue-%s-exg", name)
}

// 消息reject或nack后进入的死信（重试）队列交换机
func getRetryExgName(name string) string {
	return fmt.Sprintf("retry-%s-exg", name)
}

// 消息reject或nack后进入的死信（重试）队列
func getRetryQueueName(name string) string {
	return fmt.Sprintf("retry-%s-queue", name)
}

// 延时交换机
func getDelayExgName(name string) string {
	return fmt.Sprintf("delay-%s-exg", name)
}

//CreatePusher 创建Pusher
func CreatePusher(v Pusher) (err error) {
	if _, ok := _Pusher[v.Name]; !ok {
		_Pusher[v.Name] = v
	} else {
		return errors.New("Pusher已存在")
	}
	return nil
}

//DeletePusher 删除Pusher
func DeletePusher(name string) (err error) {
	if _, ok := _Poper[name]; ok {
		delete(_Poper, name)
	}
	return nil
}

//CreatePoper 创建Poper
func CreatePoper(v Puller) (err error) {
	if _, ok := _Poper[v.Name]; !ok {
		_Poper[v.Name] = v
	} else {
		return errors.New("Poper已存在")
	}
	return err
}

//DeletePoper 删除Poper
func DeletePoper(name string) (err error) {
	if _, ok := _Poper[name]; ok {
		delete(_Poper, name)
	}
	return nil
}

//Fini 关闭
func Fini() (err error) {
	for _, conn := range _ConnectPool {
		for _, ch := range _ChannelPool {
			if err = ch.Close(); err != nil {
				return err
			}
		}
		if err = conn.Close(); err != nil {
			return err
		}
	}
	//清空所有缓存
	Cfg = new(RabbitMQConfig)                        //配置文件对象
	_ConnectPool = make(map[string]*amqp.Connection) //连接名称:连接对象
	_ChannelPool = make(map[string]*amqp.Channel)    //信道名称:信道对象
	_ExchangePool = make(map[string]string)          //交换机名称:所属信道名称
	_QueuePool = make(map[string]string)             //队列名称:所属信道名称
	_Pusher = make(map[string]Pusher)                //Pusher名称:Pusher配置
	_Poper = make(map[string]Puller)                 //Poper名称:Poper配置

	return nil
}
