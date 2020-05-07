package util

//MSGLevel 消息等级
type MSGLevel string

const (
	// 放在header透传 用来标识消息等级
	MSGLevelKey = "level"

	// MSGLevelDiscard 消费失败 丢弃
	MSGLevelDiscard MSGLevel = "discard"
	// MSGLevelRetry 消费失败 重试
	MSGLevelRetry MSGLevel = "try"

	// 客户端发送消息时生成的唯一序列号 用于回调时找到对应的消息
	PushSequenceKey = "sequence"
)
