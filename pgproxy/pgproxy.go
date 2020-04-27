package pgproxy

import (
	"errors"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	pg "github.com/go-pg/pg/v9"
)

// ErrProxyNotInited 代理未初始化错误
var ErrProxyNotInited = errors.New("proxy not inited yet")

// ErrProxyAlreadyInited 代理已经初始化错误
var ErrProxyAlreadyInited = errors.New("proxy already inited yet")

// ErrDBURLSchemaWrong 数据库代理解析配置URL时Schema错误
var ErrDBURLSchemaWrong = errors.New("schema must be postgres")

// DBProxyCallback 数据库操作的回调函数
type dbProxyCallback func(dbCli *pg.DB) error

// DBProxy 数据库客户端的代理
type dbProxy struct {
	Ok        bool
	Options   *pg.Options
	Cli       *pg.DB
	callBacks []dbProxyCallback
}

// NewDBProxy 创建一个新的数据库客户端代理
func NewDBProxy() *dbProxy {
	proxy := new(dbProxy)
	proxy.Ok = false
	return proxy
}

// Close 关闭pg
func (proxy *dbProxy) Close() {
	if proxy.Ok {
		proxy.Cli.Close()
	}
}

// 将url解析为pg的初始化参数
func parseDBURL(address string) (*pg.Options, error) {
	result := &pg.Options{}
	u, err := url.Parse(address)
	if err != nil {
		return result, err
	}
	if u.Scheme != "postgres" {
		return result, ErrDBURLSchemaWrong
	}

	user := u.User.Username()
	if user == "" {
		result.User = "postgres"
	} else {
		result.User = user
	}
	password, has := u.User.Password()
	if has == false {
		result.Password = "postgres"
	} else {
		result.Password = password
	}
	result.Addr = u.Host
	result.Database = u.Path[1:]
	if u.RawQuery != "" {
		v, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			log.Fatal(err)
			return result, nil
		}
		for key, value := range v {
			switch strings.ToLower(key) {
			case "maxretries":
				{
					maxretries, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.MaxRetries = maxretries
					}
				}
			case "retrystatementtimeout":
				{
					bv := strings.ToLower(value[0])
					switch bv {
					case "true":
						result.RetryStatementTimeout = true
					case "false":
						result.RetryStatementTimeout = false
					default:
						log.Fatal("unknown value for RetryStatementTimeout")
					}
				}
			case "minretrybackoff":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.MinRetryBackoff = time.Duration(number) * time.Second
					}
				}
			case "maxretrybackoff":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.MaxRetryBackoff = time.Duration(number) * time.Second
					}
				}
			case "dialtimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.DialTimeout = time.Duration(number) * time.Second
					}
				}
			case "readtimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.ReadTimeout = time.Duration(number) * time.Second
					}
				}
			case "writetimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.WriteTimeout = time.Duration(number) * time.Second
					}
				}
			case "maxconnage":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.MaxConnAge = time.Duration(number) * time.Second
					}
				}
			case "pooltimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.PoolTimeout = time.Duration(number) * time.Second
					}
				}
			case "idletimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.IdleTimeout = time.Duration(number) * time.Second
					}
				}
			case "idlecheckfrequency":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.IdleCheckFrequency = time.Duration(number) * time.Second
					}
				}
			case "PoolSize":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.PoolSize = number
					}
				}
			case "MinIdleConns":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Fatal(err)
					} else {
						result.MinIdleConns = number
					}
				}
			}
		}

	}
	return result, nil
}

// Init 使用配置给代理赋值客户端实例
func (proxy *dbProxy) Init(options *pg.Options) error {
	if proxy.Ok {
		return ErrProxyAlreadyInited
	}
	proxy.Options = options
	db := pg.Connect(options)
	proxy.Cli = db
	for _, cb := range proxy.callBacks {
		//go cb(proxy.Cli)
		cb(proxy.Cli)
	}
	proxy.Ok = true
	return nil
}

// InitFromURL 使用配置给代理赋值客户端实例
func (proxy *dbProxy) InitFromURL(address string) error {
	options, err := parseDBURL(address)
	if err != nil {
		return err
	}
	err = proxy.Init(options)
	return err
}

func (proxy *dbProxy) Exec(query interface{}, params ...interface{}) (res pg.Result, err error) {
	if !proxy.Ok {
		return nil, ErrProxyNotInited
	}
	return proxy.Cli.Exec(query, params)
}

// Regist 注册回调函数,在init执行后执行回调函数
func (proxy *dbProxy) Regist(cb dbProxyCallback) {
	proxy.callBacks = append(proxy.callBacks, cb)
}

// DB 默认的etcd代理对象
var DB = NewDBProxy()
