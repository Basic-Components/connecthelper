package redisproxy

import (
	"github.com/go-redis/redis"
)

//stream 流对象
type stream struct {
	proxy  *redisProxy
	Topic  string
	MaxLen int64
	Strict bool //maxlen with ~
	offset string
}

func newStream(proxy *redisProxy, topic string, MaxLen int64, strict bool) *stream {
	s := new(stream)
	s.Topic = topic
	s.proxy = proxy
	s.MaxLen = MaxLen
	s.Strict = strict
	return s
}

//Len 查看流的长度
func (ps *stream) Len() (int64, error) {
	if !ps.proxy.IsOk() {
		return 0, ErrProxyNotInited
	}
	conn, err := ps.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.XLen(ps.Topic).Result()
}

//Delete 设置标志位标识删除指定id的数据
func (ps *stream) Delete(ids ...string) error {
	if !ps.proxy.IsOk() {
		return ErrProxyNotInited
	}
	conn, err := ps.proxy.GetConn()
	if err != nil {
		return err
	}
	_, err = conn.XDel(ps.Topic, ids...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Range 获取消息列表，会自动过滤已经删除的消息,注意-表示最小值, +表示最大值
func (ps *stream) Range(start, stop string) ([]redis.XMessage, error) {
	if !ps.proxy.IsOk() {
		return nil, ErrProxyNotInited
	}
	conn, err := ps.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	return conn.XRange(ps.Topic, start, stop).Result()
}

//Publish 向流发送消息
func (ps *stream) Publish(value map[string]interface{}) (string, error) {
	if !ps.proxy.IsOk() {
		return "", ErrProxyNotInited
	}
	conn, err := ps.proxy.GetConn()
	if err != nil {
		return "", err
	}
	args := redis.XAddArgs{
		Stream: ps.Topic,
		Values: value,
		ID:     "*",
	}
	if ps.MaxLen != 0 {
		if ps.Strict {
			args.MaxLen = ps.MaxLen
		} else {
			args.MaxLenApprox = ps.MaxLen
		}
	}
	return conn.XAdd(&args).Result()
}

//Subscribe 订阅流
func (ps *stream) Subscribe(group string) (string, error) {
	if !ps.proxy.IsOk() {
		return "", ErrProxyNotInited
	}
	conn, err := ps.proxy.GetConn()
	if err != nil {
		return "", err
	}
	args := redis.XAddArgs{
		Stream: ps.Topic,
		Values: value,
		ID:     "*",
	}
	if ps.MaxLen != 0 {
		if ps.Strict {
			args.MaxLen = ps.MaxLen
		} else {
			args.MaxLenApprox = ps.MaxLen
		}
	}
	return conn.XAdd(&args).Result()
}
