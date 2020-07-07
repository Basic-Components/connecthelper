package etcd3helper

import (
	"sync"

	log "github.com/Basic-Components/loggerhelper"

	"go.etcd.io/etcd/clientv3"
)

// etcd3HelperCallback etcdv3操作的回调函数
type etcd3HelperCallback func(cli *clientv3.Client) error

// etcd3Helper etcd3客户端的代理
type etcd3Helper struct {
	helperLock sync.RWMutex //代理的锁
	Options    *clientv3.Config
	conn       *clientv3.Client
	callBacks  []etcd3HelperCallback
}

// New 创建一个新的数据库客户端代理
func New() *etcd3Helper {
	proxy := new(etcd3Helper)
	proxy.helperLock = sync.RWMutex{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *etcd3Helper) IsOk() bool {
	if proxy.conn == nil {
		return false
	}
	return true
}

//GetConn 获取被代理的连接
func (proxy *etcd3Helper) GetConn() (*clientv3.Client, error) {
	if !proxy.IsOk() {
		return proxy.conn, ErrHelperNotInited
	}
	proxy.helperLock.RLock()
	defer proxy.helperLock.RUnlock()
	return proxy.conn, nil
}

// Close 关闭etcd
func (proxy *etcd3Helper) Close() {
	if proxy.IsOk() {
		proxy.conn.Close()
		proxy.helperLock.Lock()
		proxy.conn = nil
		proxy.helperLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *etcd3Helper) SetConnect(cli *clientv3.Client) {
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
func (proxy *etcd3Helper) Init(cli *clientv3.Client) error {
	if proxy.IsOk() {
		return ErrHelperAlreadyInited
	}
	proxy.SetConnect(cli)
	return nil
}

// InitFromOptions 从配置条件初始化代理对象
func (proxy *etcd3Helper) InitFromOptions(options *clientv3.Config) error {
	proxy.Options = options
	cli, err := clientv3.New(*options)
	if err != nil {
		return err
	}
	return proxy.Init(cli)
}

// Regist 注册回调函数,在init执行后执行回调函数
func (proxy *etcd3Helper) Regist(cb etcd3HelperCallback) {
	proxy.callBacks = append(proxy.callBacks, cb)
}

//Helper 默认的pg代理对象
var Helper = New()
