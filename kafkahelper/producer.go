package kafkahelper

import (
	"sync"

	log "github.com/Basic-Components/loggerhelper"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaProducerProxyCallback KafkaProducer操作的回调函数
type KafkaProducerProxyCallback func(producer *kafka.Producer) error

// kafkaProducerProxy kafka生产者的代理
type kafkaProducerProxy struct {
	proxyLock sync.RWMutex //代理的锁
	Options   *kafka.ConfigMap
	conn      *kafka.Producer
	callBacks []KafkaProducerProxyCallback
}

// NewProducerProxy 创建一个新的数据库客户端代理
func NewProducerProxy() *kafkaProducerProxy {
	proxy := new(kafkaProducerProxy)
	proxy.proxyLock = sync.RWMutex{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *kafkaProducerProxy) IsOk() bool {
	if proxy.conn == nil {
		return false
	}
	return true
}

//GetConn 获取被代理的连接
func (proxy *kafkaProducerProxy) GetConn() (*kafka.Producer, error) {
	if !proxy.IsOk() {
		return proxy.conn, ErrProxyNotInited
	}
	proxy.proxyLock.RLock()
	defer proxy.proxyLock.RUnlock()
	return proxy.conn, nil
}

// Close 关闭pg
func (proxy *kafkaProducerProxy) Close() {
	if proxy.IsOk() {
		proxy.conn.Close()
		proxy.proxyLock.Lock()
		proxy.conn = nil
		proxy.proxyLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *kafkaProducerProxy) SetConnect(cli *kafka.Producer) {
	proxy.proxyLock.Lock()
	proxy.conn = cli
	proxy.proxyLock.Unlock()
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
func (proxy *kafkaProducerProxy) Init(cli *kafka.Producer) error {
	if proxy.IsOk() {
		return ErrProxyAlreadyInited
	}
	proxy.SetConnect(cli)
	return nil
}

// InitFromOptions 从配置条件初始化代理对象
func (proxy *kafkaProducerProxy) InitFromOptions(options *kafka.ConfigMap) error {
	proxy.Options = options
	db, err := kafka.NewProducer(options)
	if err != nil {
		return err
	}
	return proxy.Init(db)
}

// InitFromURL 使用配置给代理赋值客户端实例
func (proxy *kafkaProducerProxy) InitFromURL(address string) error {
	options := &kafka.ConfigMap{"bootstrap.servers": address}
	err = proxy.InitFromOptions(options)
	return err
}

//Publish 快速发布消息的接口
func (proxy *kafkaProducerProxy) Publish(topic string, value []byte, key []byte) err {
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

var ProducerProxy = NewProducerProxy()
