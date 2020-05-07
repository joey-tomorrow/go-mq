 # 基于GO语言的消息队列客户端
 集成主流消息队列
 - [x] 已支持
   - [x] RabbitMQ
   - [x] Kafka
 - [x] 支持中...
    - [ ] RocketMQ
 
 # 使用方法

 使用rabbitmq
 ```
 参见 rmq_test.go
 ```

 使用kafka
  ```
  参见 kafka_test.go
  
  注意：如果需要修改配置信息，KafkaConfig中的Connect有OriginCfg属性，在这一步进行创建修改即可。
  如果不设置OriginCfg,则会使用默认的配置信息。
  ```
 ## 接口
 ```
 //IMessageQueue 消息队列总接口
 type IMessageQueue interface {
 	// 同步发送接口
 	// @param pusher 标识使用哪个发送者
 	// @param 发送的数据
 	SyncPush(pusher string, data mqutil.MSG) error
 
 	// 异步回调发送接口
 	// @param pusher 标识使用哪个发送者
 	// @param 发送的数据
 	AsyncPush(pusher string, data mqutil.MSG, callback mqutil.Callback) error
 
 	// 单向发送接口 不关心是否真的发送到mq
 	// @param pusher 标识使用哪个发送者
 	// @param 发送的数据
 	OneWay(pusher string, data mqutil.MSG) error
 
 	// 延时发送接口
 	// @param pusher 标识使用哪个发送者
 	// @param 发送的数据
 	// @param delay 延时时间*毫秒
 	DelayPush(pusher string, data mqutil.MSG, delay int64) error
 
 	// 事务发送接口
 	// @param pusher 标识使用哪个发送者
 	// @param 发送的数据
 	TxPush(pusher string, data mqutil.MSG, fun func() error) error
 
 	// 消费接口
 	// @param poper 标识使用哪个消费者
 	// @param callback 回调函数
 	Consume(poper string, callback func(mqutil.MSG) error) error
 }

 ```
 ## RabbitMQ
 + 消息发送方断线重连
 + 消息接收方断线重连
 + 同步消息
 + 异步消息
 + 异步回调
 + 事务消息
 + 延时队列
 ```
 RabbitMQ的延时消息实现方式有三种
 1、延时消息 懒过期，消息延时精确度非常低
 2、延时队列 需要另外创建一个队列
 3、延时插件 最为便捷，创建一个延时交换机即可
 
 所以默认使用第三种方式实现，如果需要延时队列，则服务端需要安装一个延时插件（安装非常简单）
 ```
 
 ### 特别说明：
 ```
 MSG可以设置是否持久化，MQ服务器重启后消息会丢失。
 持久化消息会严重影响Rabbitmq性能，建议按照业务来设置是否持久化。
 ```
 
   ```
   如果使用事务发送消息，则需要配置一个单独的channel，例如：
   {Name: txChannel, Connect: myConnect, QosCount: 1, QosSize: 0, ConfirmNoWait: true}
   
   然后再配置一个单独的pusher即可：
   {Name: myTxPusher, Channel: txChannel, Exchange: myExg03, Key: myQueue01, Mandtory: false,
        Immediate: false, ContentType: text/plain, DeliveryMode: 0}
   ```
   
 ### 小技巧： 
 + 当发生MQ故障或者消费者出现问题时，可能会造成队列堆积消息过多。这时可以定义多个channel+poper来加速消费速度。
 + 建议生产者、消费者配置不同的connection，因为MQ有些熔断机制会触发阻塞消息生产者，而不阻塞消费者。
 
 ### TODO：
 + 通过每个队列建立多个默认队列来优化性能（注意需要顺序性消息的处理）
 
 配置文件详见【rmq-template.yaml】
 
  ## Kafka
  + 同步消息
  + 异步消息
  + 异步回调
  + 断线重连
 ```
 目前的saram客户端还不支持事务。
 Kafka的配置相对比较复杂，因此目前使用了一个默认的配置来启用Kafka。
 如果只是简单的收发消息，是没有问题的。
 如果想配置更多的选项，比如acks、retry、infight，也预留了一个口子可以进行配置（见使用方法说明）
  ```
 ### TODO：
 + 事务消息
 
 配置文件详见【kafka-template.yaml】