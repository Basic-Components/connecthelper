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

// ErrLockAlreadyReleased 分布式锁已经被释放了
var ErrLockAlreadyReleased = errors.New("Lock is already released")

//ErrLockWaitTimeout 等待解锁超时
var ErrLockWaitTimeout = errors.New("wait lock timeout")

// Done 锁的结束信号
var Done = errors.New("done")
