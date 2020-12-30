# connectproxy

各种通用组件连接的代理.

各个子模块用于代理一种常用服务.都使用`New`方法创建代理对象,使用`Init`初始化被代理的对象.由于代理的都是有url的,因此也都会提供一个`InitFromURL`方法用于从url直接初始化.

## 代理的对象汇总

| 子模块       | 代理的包                                           | 默认的代理对象                                         |
| ------------ | -------------------------------------------------- | ------------------------------------------------------ |
| `pghelper`    | `github.com/go-pg/pg/v10`                           | `Helper`                                                |
| `etcd3helper` | `go.etcd.io/etcd/clientv3`                         | `Helper`                                                |
| `redishelper` | `github.com/go-redis/redis/v8`                        | `Helper`                                                |
| `kafkahelper` | `github.com/confluentinc/confluent-kafka-go/kafka` | `ProducerHelper|ConsumerHelper`                          |

## ChangeLog

### v0.0.6

删除对etcd的支持

### v0.0.5

替换log包

### v0.0.4

+ 实现了基本功能