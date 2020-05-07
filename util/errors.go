package util

import (
	"errors"
)

var (
	//ErrMsgTimeout 消息发送超时
	ErrMsgTimeout = errors.New("mq: msg publish timeout")
	// ErrMethodNotSupport 方法不支持
	ErrMethodNotSupport = errors.New("mq: method not support")
	//ErrShutdownHook 协程panic
	ErrShutdownHook = errors.New("mq: async publish goroutine shutdown")
	//ErrMsgNotConfirmed 消息发送失败
	ErrMsgNotConfirmed = errors.New("mq: msg publish to exchange failed")
	//ErrMsgEmpty 发送消息为空
	ErrMsgEmpty = errors.New("mq: message can not be empty")
)
