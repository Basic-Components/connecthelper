package kafkahelper

import (
	"sync"

	log "github.com/Basic-Components/loggerhelper"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConsumerHelperCallback KafkaProducer操作的回调函数
type KafkaConsumerHelperCallback func(consumer *kafka.Consumer) error

// kafkaProducerHelper kafka生产者的代理
type kafkaConsumerHelper struct {
	helperLock sync.RWMutex //代理的锁
	lock       sync.WaitGroup
	Topics     []string
	Options    *kafka.ConfigMap
	conns      []*kafka.Consumer
	Workers    int32
	callBacks  []KafkaConsumerHelperCallback
}

// NewConsumerHelper 创建一个kafka客户端代理
func NewConsumerHelper() *kafkaConsumerHelper {
	proxy := new(kafkaConsumerHelper)
	proxy.conns = []*kafka.Consumer{}
	proxy.helperLock = sync.RWMutex{}
	proxy.lock = sync.WaitGroup{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *kafkaConsumerHelper) IsOk() bool {
	if len(proxy.conns) == 0 {
		return false
	}
	return true
}

//GetConns 获取被代理的连接
func (proxy *kafkaConsumerHelper) GetConns() ([]*kafka.Consumer, error) {
	if !proxy.IsOk() {
		return proxy.conns, ErrHelperNotInited
	}
	proxy.helperLock.RLock()
	defer proxy.helperLock.RUnlock()
	return proxy.conns, nil
}

// Close 关闭kafka监听
func (proxy *kafkaConsumerHelper) Close() {
	if proxy.IsOk() {
		proxy.helperLock.Lock()
		for _, conn := range proxy.conns {
			conn.Close()
		}
		proxy.conns = nil
		proxy.helperLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *kafkaConsumerHelper) AppendConnect(cli *kafka.Consumer) {
	proxy.helperLock.Lock()
	proxy.conns = append(proxy.conns, cli)
	proxy.helperLock.Unlock()
	for _, cb := range proxy.callBacks {
		err := cb(cli)
		if err != nil {
			log.Error(map[string]interface{}{"err": err}, "regist callback get error")
		} else {
			log.Info(nil, "regist callback done")
		}
	}
}

// Init 给代理赋值客户端实例
func (proxy *kafkaConsumerHelper) Init(clis []*kafka.Consumer) error {
	if proxy.IsOk() {
		return ErrHelperAlreadyInited
	}
	for _, cli := range clis {
		proxy.AppendConnect(cli)
	}

	return nil
}

// InitFromOptions 从配置条件初始化代理对象
func (proxy *kafkaConsumerHelper) InitFromOptions(options *kafka.ConfigMap, topics []string, workers int32) error {
	proxy.Options = options
	proxy.Workers = workers
	proxy.Topics = topics
	clis := []*kafka.Consumer{}
	for i := int32(0); i < workers; i++ {
		db, err := kafka.NewConsumer(options)
		if err != nil {
			return err
		}
		clis = append(clis, db)
	}
	return proxy.Init(clis)
}

// InitFromURL 使用配置给代理赋值客户端实例
func (proxy *kafkaConsumerHelper) InitFromURL(address, groupid, auto_offset_reset, isolation_level string, topics []string, workers int32) error {
	options := &kafka.ConfigMap{
		"bootstrap.servers": address,
		"group.id":          groupid,
		"auto.offset.reset": auto_offset_reset,
		"isolation.level":   isolation_level,
	}
	err := proxy.InitFromOptions(options, topics, workers)
	return err
}

//KafkaConsumerHanddler kafka消费端的处理程序
type KafkaConsumerHanddler func(msg *kafka.Message)

func (proxy *kafkaConsumerHelper) listener(index int, Consumer *kafka.Consumer, handdler KafkaConsumerHanddler) {
	Consumer.SubscribeTopics(proxy.Topics, nil)
	defer Consumer.Unsubscribe()
	log.Info(map[string]interface{}{"no": index}, "start listening")
	for {
		msg, err := Consumer.ReadMessage(-1)
		if err != nil {
			log.Error(map[string]interface{}{"err": err}, "get msg error")
		}
		go handdler(msg)
	}
	proxy.lock.Done()
}

func (proxy *kafkaConsumerHelper) Listen(handdler KafkaConsumerHanddler) {
	for index, Consumer := range proxy.conns {
		proxy.lock.Add(1)
		go proxy.listener(index, Consumer, handdler)
	}
	proxy.lock.Wait()
}

var ConsumerHelper = NewConsumerHelper()
