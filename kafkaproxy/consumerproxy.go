package kafkaproxy

import (
	"log"
	"sync"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConsumerProxyCallback KafkaProducer操作的回调函数
type KafkaConsumerProxyCallback func(consumer *kafka.Consumer) error

// kafkaProducerProxy kafka生产者的代理
type kafkaConsumerProxy struct {
	proxyLock sync.RWMutex //代理的锁
	Options   *kafka.ConfigMap
	conn      *kafka.Consumer
	callBacks []KafkaConsumerProxyCallback
}

// NewConsumerProxy 创建一个新的数据库客户端代理
func NewConsumerProxy() *kafkaConsumerProxy {
	proxy := new(kafkaConsumerProxy)
	proxy.proxyLock = sync.RWMutex{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *kafkaConsumerProxy) IsOk() bool {
	if proxy.conn == nil {
		return false
	}
	return true
}

//GetConn 获取被代理的连接
func (proxy *kafkaConsumerProxy) GetConn() (*kafka.Consumer, error) {
	if !proxy.IsOk() {
		return proxy.conn, ErrProxyNotInited
	}
	proxy.proxyLock.RLock()
	defer proxy.proxyLock.RUnlock()
	return proxy.conn, nil
}

// Close 关闭pg
func (proxy *kafkaConsumerProxy) Close() {
	if proxy.IsOk() {
		proxy.conn.Close()
		proxy.proxyLock.Lock()
		proxy.conn = nil
		proxy.proxyLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *kafkaConsumerProxy) SetConnect(cli *kafka.Consumer) {
	proxy.proxyLock.Lock()
	proxy.conn = cli
	proxy.proxyLock.Unlock()
	for _, cb := range proxy.callBacks {
		err := cb(proxy.conn)
		if err != nil {
			log.Println("regist callback get error", err)
		} else {
			log.Println("regist callback done")
		}
	}
}

// Init 给代理赋值客户端实例
func (proxy *kafkaConsumerProxy) Init(cli *kafka.Consumer) error {
	if proxy.IsOk() {
		return ErrProxyAlreadyInited
	}
	proxy.SetConnect(cli)
	return nil
}

// InitFromOptions 从配置条件初始化代理对象
func (proxy *kafkaConsumerProxy) InitFromOptions(options *kafka.ConfigMap) error {
	proxy.Options = options
	db, err := kafka.NewProducer(options)
	if err != nil {
		return err
	}
	return proxy.Init(db)
}

// InitFromURL 使用配置给代理赋值客户端实例
func (proxy *kafkaConsumerProxy) InitFromURL(address, groupid, auto_offset_reset, isolation_level string) error {
	options := &kafka.ConfigMap{
		"bootstrap.servers": address,
		"group.id":          groupid,
		"auto.offset.reset": auto_offset_reset,
		"isolation.level":   isolation_level,
	}
	err = proxy.InitFromOptions(options)
	return err
}

// Subscribe 使用配置给代理赋值客户端实例
func (proxy *kafkaConsumerProxy) Subscribe(topics []string) error {
	p, err := proxy.GetConn()
	if err != nil {
		return nil, err
	}
	p.SubscribeTopics(topics, nil)
}

// UnSubscribe 使用配置给代理赋值客户端实例
func (proxy *kafkaConsumerProxy) UnSubscribe() error {
	p, err := proxy.GetConn()
	if err != nil {
		return nil, err
	}
	return p.UnSubscribe()
}

var ConsumerProxy = NewConsumerProxy()
