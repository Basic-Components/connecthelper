package etcd3helper

import (
	"errors"
)

// ErrHelperNotInited 代理未初始化错误
var ErrHelperNotInited = errors.New("proxy not inited yet")

// ErrHelperAlreadyInited 代理已经初始化错误
var ErrHelperAlreadyInited = errors.New("proxy already inited yet")
