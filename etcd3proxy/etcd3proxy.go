package etcd3proxy

import (
	"log"

	"github.com/Basic-Components/connectproxy/errs"
	"go.etcd.io/etcd/clientv3"
)

// etcd3ProxyCallback etcdv3操作的回调函数
type etcd3ProxyCallback func(cli *clientv3.Client) error

// etcd3Proxy etcd3客户端的代理
type etcd3Proxy struct {
	Ok        bool
	Options   *clientv3.Config
	Cli       *clientv3.Client
	callBacks []etcd3ProxyCallback
}

// New 创建一个新的数据库客户端代理
func New() *etcd3Proxy {
	proxy := new(etcd3Proxy)
	proxy.Ok = false
	return proxy
}

// Close 关闭pg
func (proxy *etcd3Proxy) Close() {
	if proxy.Ok {
		proxy.Cli.Close()
	}
}

// Init 给代理赋值客户端实例
func (proxy *etcd3Proxy) Init(cli *clientv3.Client) error {
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
func (proxy *etcd3Proxy) InitFromOptions(options *clientv3.Config) error {
	proxy.Options = options
	cli, err := clientv3.New(*options)
	if err != nil {
		return err
	}
	return proxy.Init(cli)
}

// Regist 注册回调函数,在init执行后执行回调函数
func (proxy *etcd3Proxy) Regist(cb etcd3ProxyCallback) {
	proxy.callBacks = append(proxy.callBacks, cb)
}

// Etcd 默认的pg代理对象
var Etcd = New()
