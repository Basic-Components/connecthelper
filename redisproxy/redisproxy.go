package redisproxy

import (
	"log"

	"sync"

	"github.com/go-redis/redis"
)

// redisProxyCallback etcdv3操作的回调函数
type redisProxyCallback func(cli *redis.Client) error

// redisProxy redis客户端的代理
type redisProxy struct {
	proxyLock sync.RWMutex //代理的锁
	Options   *redis.Options
	conn      *redis.Client
	callBacks []redisProxyCallback
}

// New 创建一个新的数据库客户端代理
func New() *redisProxy {
	proxy := new(redisProxy)
	proxy.proxyLock = sync.RWMutex{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *redisProxy) IsOk() bool {
	if proxy.conn == nil {
		return false
	}
	return true
}

func (proxy *redisProxy) GetConn() (*redis.Client, error) {
	if !proxy.IsOk() {
		return proxy.conn, ErrProxyNotInited
	}
	proxy.proxyLock.RLock()
	defer proxy.proxyLock.RUnlock()
	return proxy.conn, nil
}

// Close 关闭pg
func (proxy *redisProxy) Close() {
	if proxy.IsOk() {
		proxy.conn.Close()
		proxy.proxyLock.Lock()
		proxy.conn = nil
		proxy.proxyLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *redisProxy) SetConnect(cli *redis.Client) {
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
func (proxy *redisProxy) Init(cli *redis.Client) error {
	if proxy.IsOk() {
		return ErrProxyAlreadyInited
	}
	proxy.SetConnect(cli)
	return nil
}

// InitFromOptions 从配置条件初始化代理对象
func (proxy *redisProxy) InitFromOptions(options *redis.Options) error {
	proxy.Options = options
	cli := redis.NewClient(options)
	return proxy.Init(cli)
}

// InitFromURL 从URL条件初始化代理对象
func (proxy *redisProxy) InitFromURL(url string) error {
	options, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	return proxy.InitFromOptions(options)
}

// Regist 注册回调函数,在init执行后执行回调函数
func (proxy *redisProxy) Regist(cb redisProxyCallback) {
	proxy.callBacks = append(proxy.callBacks, cb)
}

//NewLock 创建一个全局锁
func (proxy *redisProxy) NewLock(key string, timeout int64) *distributedLock {
	lock := newLock(proxy, key, timeout)
	return lock
}

//NewCounter 创建一个全局锁
func (proxy *redisProxy) NewCounter(key string) *distributedcounter {
	counter := newCounter(proxy, key)
	return counter
}

//NewBitmap 创建一个位图
func (proxy *redisProxy) NewBitmap(key string) *bitmap {
	bm := newBitmap(proxy, key)
	return bm
}

// Redis 默认的pg代理对象
var Redis = New()
