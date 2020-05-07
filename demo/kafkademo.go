package main

import (
	"fmt"
	"go-mq/kafka"
	"go-mq/util"
	"time"
)

func main() {
	kafka, err := kafka.InitKafkaFromPath("./conf/kafka.yml")
	if err != nil {
		panic(err)
	}

	_ = kafka.Consume("myPoper", func(msg util.MSG) error {
		fmt.Println("receive msg success:", string(msg.Body))
		return nil
	})

	_ = kafka.SyncPush("myPusher", util.MSG{
		Body: []byte(fmt.Sprintf("sync push")),
	})

	_ = kafka.AsyncPush("myPusher", util.MSG{
		Body: []byte(fmt.Sprintf("async push")),
	}, func(msg util.MSG, e error) {
		fmt.Println("async success: ", string(msg.Body))
	})

	_ = kafka.OneWay("myPusher", util.MSG{
		Body: []byte(fmt.Sprintf("one way")),
	})

	time.Sleep(50 * time.Second)
}
