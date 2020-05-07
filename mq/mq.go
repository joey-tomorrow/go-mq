package mq

import "go-mq/util"

//IMessageQueue 消息队列总接口
type IMessageQueue interface {
	// 同步发送接口
	// @param pusher 标识使用哪个发送者
	// @param 发送的数据
	SyncPush(pusher string, data util.MSG) error

	// 异步回调发送接口
	// @param pusher 标识使用哪个发送者
	// @param 发送的数据
	AsyncPush(pusher string, data util.MSG, callback util.Callback) error

	// 单向发送接口 不关心是否真的发送到mq
	// @param pusher 标识使用哪个发送者
	// @param 发送的数据
	OneWay(pusher string, data util.MSG) error

	// 延时发送接口
	// @param pusher 标识使用哪个发送者
	// @param 发送的数据
	// @param delay 延时时间*毫秒
	DelayPush(pusher string, data util.MSG, delay int64) error

	// 事务发送接口
	// @param pusher 标识使用哪个发送者
	// @param 发送的数据
	TxPush(pusher string, data util.MSG, fun func() error) error

	// 消费接口
	// @param poper 标识使用哪个消费者
	// @param callback 回调函数
	Consume(poper string, callback func(util.MSG) error) error
}
