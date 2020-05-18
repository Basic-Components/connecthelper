package pgproxy

import (
	"errors"
)

// ErrProxyNotInited 代理未初始化错误
var ErrProxyNotInited = errors.New("proxy not inited yet")

// ErrProxyAlreadyInited 代理已经初始化错误
var ErrProxyAlreadyInited = errors.New("proxy already inited yet")

// ErrURLSchemaWrong 分布式锁已经设置
var ErrURLSchemaWrong = errors.New("URL schema wrong")
