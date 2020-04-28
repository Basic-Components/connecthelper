package redisproxy

import (
	"log"

	"github.com/Basic-Components/connectproxy/errs"
	"github.com/go-redis/redis"
)

// redisProxyCallback etcdv3操作的回调函数
type redisProxyCallback func(cli *redis.Client) error

// redisProxy redis客户端的代理
type redisProxy struct {
	Ok        bool
	Options   *redis.Options
	Cli       *redis.Client
	callBacks []redisProxyCallback
}

// New 创建一个新的数据库客户端代理
func New() *redisProxy {
	proxy := new(redisProxy)
	proxy.Ok = false
	return proxy
}

// Close 关闭pg
func (proxy *redisProxy) Close() {
	if proxy.Ok {
		proxy.Cli.Close()
	}
}

// Init 给代理赋值客户端实例
func (proxy *redisProxy) Init(cli *redis.Client) error {
	if proxy.Ok {
		return errs.ErrProxyAlreadyInited
	}
	proxy.Cli = cli
	for _, cb := range proxy.callBacks {
		err := cb(proxy.Cli)
		if err != nil {
			log.Println("regist callback get error", err)
		} else {
			log.Println("regist callback done")
		}
	}
	proxy.Ok = true
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

// Redis 默认的pg代理对象
var Redis = New()
