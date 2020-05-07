package kafka

import (
	"github.com/Shopify/sarama"
	"go-mq/util"
	"gopkg.in/yaml.v2"
	"hidevops.io/hiboot/pkg/utils/cmap"
	"io/ioutil"
	"os"
	"strings"
)

//InitKafkaFromPath 从配置文件初始化
func InitKafkaFromPath(path string) (kafka *Kafka, err error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}

	return InitKafka(cfg)
}

//LoadConfig 读取配置文件
func LoadConfig(path string) (*KafkaConfig, error) {
	fp, err := os.Open(path)
	if err != nil {
		kafkaError.PrintlnErrorMessage("load config from path:%s failed:%v", path, err)
		return nil, err
	}

	defer fp.Close()

	var (
		data []byte
		cfg  = &KafkaConfig{}
	)

	if data, err = ioutil.ReadAll(fp); err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

//InitKafka 从配置结构体初始化
//可以调用LoadConfig获取配置后，修改Connect中的kafka扩展配置，然后再调用此方法初始化
func InitKafka(cfg *KafkaConfig) (kafka *Kafka, err error) {
	Cfg = cfg

	_ConfirmTimeout = cfg.ConfirmTimeout
	if cfg.ConfirmTimeout <= 0 {
		_ConfirmTimeout = 10
	}

	if err = initConnect(); err != nil {
		return
	}

	if err = initPusher(); err != nil {
		return
	}

	if err = initPoper(); err != nil {
		return
	}

	callbackProducer()

	return NewKafka(), nil
}

func initConnect() (err error) {
	for _, v := range Cfg.Connects {
		if v.OriginCfg == nil {
			v.OriginCfg = sarama.NewConfig()
		}
		_ConnectPool[v.Name] = v
	}

	return nil
}

func initPusher() (err error) {
	for _, v := range Cfg.Pushers {
		_PusherPool[v.Name] = v
		//connect, err := getConnect(v.Topic)
		//if err != nil {
		//	return err
		//}
		connect := _ConnectPool[v.Connect]

		if pusher := os.Getenv("mock-mq"); pusher != "" {
			_DeliveryMap[pusher] = &util.Delivery{
				Channel:     pusher,
				DeliveryTag: 0,
				DeliveryMap: cmap.New(),
			}
			return nil
		}

		connect.OriginCfg.Producer.Return.Successes = true
		connect.OriginCfg.Version = sarama.V0_11_0_2
		asyncPusher, err := sarama.NewAsyncProducer(strings.Split(connect.Addr, ","), connect.OriginCfg)
		if err != nil {
			return err
		}

		AsyncPusherPool[v.Name] = asyncPusher

		syncPusher, err := sarama.NewSyncProducer(strings.Split(connect.Addr, ","), connect.OriginCfg)
		if err != nil {
			return err
		}
		SyncPusherPool[v.Name] = syncPusher

		_DeliveryMap[v.Name] = &util.Delivery{
			Channel:     v.Name,
			DeliveryTag: 0,
			DeliveryMap: cmap.New(),
		}
	}

	return nil
}

func initPoper() (err error) {
	for _, v := range Cfg.Popers {
		_PoperPool[v.Name] = v
	}

	return nil
}
