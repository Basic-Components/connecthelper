# connectproxy

各种通用组件连接的代理.

各个子模块用于代理一种常用服务.都使用`New`方法创建代理对象,使用`Init`初始化被代理的对象.由于代理的都是有url的,因此也都会提供一个`InitFromURL`方法用于从url直接初始化.

## 代理的对象汇总

子模块|代理的包|默认的代理对象
---|---|---
`pgproxy`|`github.com/go-pg/pg/v9`|`DB`
`etcd3proxy`|`go.etcd.io/etcd/clientv3`|`Etcd`
`redisproxy`|`github.com/go-redis/redis`|`Redis`