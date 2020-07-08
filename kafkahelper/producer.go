package kafkahelper

import (
	"sync"

	log "github.com/Basic-Components/loggerhelper"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaProducerHelperCallback KafkaProducer操作的回调函数
type KafkaProducerHelperCallback func(producer *kafka.Producer) error

// kafkaProducerHelper kafka生产者的代理
type kafkaProducerHelper struct {
	helperLock sync.RWMutex //代理的锁
	Options    *kafka.ConfigMap
	conn       *kafka.Producer
	callBacks  []KafkaProducerHelperCallback
}

// NewProducerHelper 创建一个新的数据库客户端代理
func NewProducerHelper() *kafkaProducerHelper {
	proxy := new(kafkaProducerHelper)
	proxy.helperLock = sync.RWMutex{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *kafkaProducerHelper) IsOk() bool {
	if proxy.conn == nil {
		return false
	}
	return true
}

//GetConn 获取被代理的连接
func (proxy *kafkaProducerHelper) GetConn() (*kafka.Producer, error) {
	if !proxy.IsOk() {
		return proxy.conn, ErrHelperNotInited
	}
	proxy.helperLock.RLock()
	defer proxy.helperLock.RUnlock()
	return proxy.conn, nil
}

// Close 关闭pg
func (proxy *kafkaProducerHelper) Close() {
	if proxy.IsOk() {
		proxy.conn.Close()
		proxy.helperLock.Lock()
		proxy.conn = nil
		proxy.helperLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *kafkaProducerHelper) SetConnect(cli *kafka.Producer) {
	proxy.helperLock.Lock()
	proxy.conn = cli
	proxy.helperLock.Unlock()
	for _, cb := range proxy.callBacks {
		err := cb(proxy.conn)
		if err != nil {
			log.Error(map[string]interface{}{"err": err}, "regist callback get error")
		} else {
			log.Info(nil, "regist callback done")
		}
	}
}

// Init 给代理赋值客户端实例
func (proxy *kafkaProducerHelper) Init(cli *kafka.Producer) error {
	if proxy.IsOk() {
		return ErrHelperAlreadyInited
	}
	proxy.SetConnect(cli)
	return nil
}

// InitFromOptions 从配置条件初始化代理对象
func (proxy *kafkaProducerHelper) InitFromOptions(options *kafka.ConfigMap) error {
	proxy.Options = options
	db, err := kafka.NewProducer(options)
	if err != nil {
		return err
	}
	return proxy.Init(db)
}

// InitFromURL 使用配置给代理赋值客户端实例
func (proxy *kafkaProducerHelper) InitFromURL(address string, batch_producer bool, queue_buffer_max_ms, acks int) error {
	options := &kafka.ConfigMap{
		"bootstrap.servers":      address,
		"go.delivery.reports":    false,
		"go.batch.producer":      batch_producer,
		"queue.buffering.max.ms": queue_buffer_max_ms,
		"acks":                   acks,
	}
	err = proxy.InitFromOptions(options)
	return err
}

func (proxy *kafkaProducerHelper) ListenResult() error {
	p, err := proxy.GetConn()
	if err != nil {
		return err
	}
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			{
				if ev.TopicPartition.Error != nil {
					log.Error(map[string]interface{}{"KafkaURL": ins.URLS, "TopicPartition": ev.TopicPartition}, "Delivery failed to kafka")
				} else {
					log.Info(map[string]interface{}{"KafkaURL": ins.URLS, "TopicPartition": ev.TopicPartition}, "Delivered message to kafka")
				}
			}
		default:
			{
				log.Error(map[string]interface{}{"ev": ev}, "kafka producer Ignored event")
			}
		}
	}
	return nil
}

//PublishSync 快速发布消息的同步接口
func (proxy *kafkaProducerHelper) PublishSync(topic string, value []byte, key []byte) error {
	p, err := proxy.GetConn()
	if err != nil {
		return err
	}
	if key == nil {
		return p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
		}, nil)
	} else {
		return p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
			Key:            key,
		}, nil)
	}
}

func (proxy *kafkaProducerHelper) publishAsync(topic string, key []byte, value []byte) error {
	p, err := proxy.GetConn()
	if err != nil {
		return err
	}
	var msg kafka.Message
	if key != nil {
		msg = kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
			Key:            key,
		}
	} else {
		msg = kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
		}
	}
	p.ProduceChannel() <- &msg
}

//PublishAsync 快速发布消息的异步接口
func (proxy *kafkaProducerHelper) PublishAsync(topic string, value []byte) {
	go proxy.publishAsync(topic, value)
}

var ProducerHelper = NewProducerHelper()
