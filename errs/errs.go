package errs

import (
	"errors"
)

// ErrProxyNotInited 代理未初始化错误
var ErrProxyNotInited = errors.New("proxy not inited yet")

// ErrProxyAlreadyInited 代理已经初始化错误
var ErrProxyAlreadyInited = errors.New("proxy already inited yet")

// ErrURLSchemaWrong 数据库代理解析配置URL时Schema错误
var ErrURLSchemaWrong = errors.New("schema wrong")
