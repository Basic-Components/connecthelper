package pghelper

import (
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Basic-Components/loggerhelper"

	pg "github.com/go-pg/pg/v10"
)

// DBHelperCallback pg数据库操作的回调函数
type dbHelperCallback func(dbCli *pg.DB) error

// DBHelper 数据库客户端的代理
type dbHelper struct {
	helperLock sync.RWMutex //代理的锁
	Options    *pg.Options
	conn       *pg.DB
	callBacks  []dbHelperCallback
}

// New 创建一个新的数据库客户端代理
func New() *dbHelper {
	proxy := new(dbHelper)
	proxy.helperLock = sync.RWMutex{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *dbHelper) IsOk() bool {
	if proxy.conn == nil {
		return false
	}
	return true
}

//GetConn 获取被代理的连接
func (proxy *dbHelper) GetConn() (*pg.DB, error) {
	if !proxy.IsOk() {
		return proxy.conn, ErrHelperNotInited
	}
	proxy.helperLock.RLock()
	defer proxy.helperLock.RUnlock()
	return proxy.conn, nil
}

// Close 关闭pg
func (proxy *dbHelper) Close() {
	if proxy.IsOk() {
		proxy.conn.Close()
		proxy.helperLock.Lock()
		proxy.conn = nil
		proxy.helperLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *dbHelper) SetConnect(cli *pg.DB) {
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

// 将url解析为pg的初始化参数
func parseDBURL(address string) (*pg.Options, error) {
	result := &pg.Options{}
	u, err := url.Parse(address)
	if err != nil {
		return result, err
	}
	if u.Scheme != "postgres" {
		return result, ErrURLSchemaWrong
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
			log.Error(map[string]interface{}{"err": err}, "ParseQuery error")
			return result, nil
		}
		for key, value := range v {
			switch strings.ToLower(key) {
			case "maxretries":
				{
					maxretries, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "maxretries error")
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
						log.Error(nil, "unknown value for RetryStatementTimeout")
					}
				}
			case "minretrybackoff":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "minretrybackoff error")
					} else {
						result.MinRetryBackoff = time.Duration(number) * time.Second
					}
				}
			case "maxretrybackoff":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "maxretrybackoff error")
					} else {
						result.MaxRetryBackoff = time.Duration(number) * time.Second
					}
				}
			case "dialtimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "dialtimeout error")
					} else {
						result.DialTimeout = time.Duration(number) * time.Second
					}
				}
			case "readtimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "readtimeout error")
					} else {
						result.ReadTimeout = time.Duration(number) * time.Second
					}
				}
			case "writetimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "writetimeout error")
					} else {
						result.WriteTimeout = time.Duration(number) * time.Second
					}
				}
			case "maxconnage":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "maxconnage error")
					} else {
						result.MaxConnAge = time.Duration(number) * time.Second
					}
				}
			case "pooltimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "pooltimeout error")
					} else {
						result.PoolTimeout = time.Duration(number) * time.Second
					}
				}
			case "idletimeout":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "idletimeout error")
					} else {
						result.IdleTimeout = time.Duration(number) * time.Second
					}
				}
			case "idlecheckfrequency":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "idlecheckfrequency error")
					} else {
						result.IdleCheckFrequency = time.Duration(number) * time.Second
					}
				}
			case "poolsize":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "PoolSize error")
					} else {
						result.PoolSize = number
					}
				}
			case "minidleconns":
				{
					number, err := strconv.Atoi(value[0])
					if err != nil {
						log.Error(map[string]interface{}{"err": err, "value": value}, "minidleconns error")
					} else {
						result.MinIdleConns = number
					}
				}
			}
		}
	}
	return result, nil
}

// Init 给代理赋值客户端实例
func (proxy *dbHelper) Init(cli *pg.DB) error {
	if proxy.IsOk() {
		return ErrHelperAlreadyInited
	}
	proxy.SetConnect(cli)
	return nil
}

// InitFromOptions 从配置条件初始化代理对象
func (proxy *dbHelper) InitFromOptions(options *pg.Options) error {
	proxy.Options = options
	db := pg.Connect(options)
	return proxy.Init(db)
}

// InitFromURL 使用配置给代理赋值客户端实例
func (proxy *dbHelper) InitFromURL(address string) error {
	options, err := parseDBURL(address)
	if err != nil {
		return err
	}
	err = proxy.InitFromOptions(options)
	return err
}

func (proxy *dbHelper) Exec(query interface{}, params ...interface{}) (res pg.Result, err error) {
	if !proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	return proxy.conn.Exec(query, params)
}

// Regist 注册回调函数,在init执行后执行回调函数
func (proxy *dbHelper) Regist(cb dbHelperCallback) {
	proxy.callBacks = append(proxy.callBacks, cb)
}

//Helper 默认的pg代理对象
var Helper = New()
