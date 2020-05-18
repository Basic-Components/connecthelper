package etcd3proxy

import (
	"log"
	"sync"

	"go.etcd.io/etcd/clientv3"
)

// etcd3ProxyCallback etcdv3操作的回调函数
type etcd3ProxyCallback func(cli *clientv3.Client) error

// etcd3Proxy etcd3客户端的代理
type etcd3Proxy struct {
	proxyLock sync.RWMutex //代理的锁
	Options   *clientv3.Config
	conn      *clientv3.Client
	callBacks []etcd3ProxyCallback
}

// New 创建一个新的数据库客户端代理
func New() *etcd3Proxy {
	proxy := new(etcd3Proxy)
	proxy.proxyLock = sync.RWMutex{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *etcd3Proxy) IsOk() bool {
	if proxy.conn == nil {
		return false
	}
	return true
}

//GetConn 获取被代理的连接
func (proxy *etcd3Proxy) GetConn() (*clientv3.Client, error) {
	if !proxy.IsOk() {
		return proxy.conn, ErrProxyNotInited
	}
	proxy.proxyLock.RLock()
	defer proxy.proxyLock.RUnlock()
	return proxy.conn, nil
}

// Close 关闭etcd
func (proxy *etcd3Proxy) Close() {
	if proxy.IsOk() {
		proxy.conn.Close()
		proxy.proxyLock.Lock()
		proxy.conn = nil
		proxy.proxyLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *etcd3Proxy) SetConnect(cli *clientv3.Client) {
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
func (proxy *etcd3Proxy) Init(cli *clientv3.Client) error {
	if proxy.IsOk() {
		return ErrProxyAlreadyInited
	}
	proxy.SetConnect(cli)
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

//Proxy 默认的pg代理对象
var Proxy = New()
