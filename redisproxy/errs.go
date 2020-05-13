package redisproxy

import (
	"errors"
)

// ErrProxyNotInited 代理未初始化错误
var ErrProxyNotInited = errors.New("proxy not inited yet")

// ErrProxyAlreadyInited 代理已经初始化错误
var ErrProxyAlreadyInited = errors.New("proxy already inited yet")

// ErrLockAlreadySet 分布式锁已经设置
var ErrLockAlreadySet = errors.New("Lock is already setted")

//ErrLockWaitTimeout 等待解锁超时
var ErrLockWaitTimeout = errors.New("wait lock timeout")

//ErrKeyNotExist 键不存在
var ErrKeyNotExist = errors.New("key not exist")

//ErrGroupNotInTopic 消费组不在topic上
var ErrGroupNotInTopic = errors.New("Group Not In Topic")

// Done 锁的结束信号
var Done = errors.New("done")
