## dubbo中zk的应用
```java
在zk中创建的节点路径
dubbo
    com.alibaba.demo.DemoService [服务名,service]
        configurators
        consumers [类型,type,生产者消费者]
            ...
        providers
            ... [url]
        routers    
```
其中接口(service)下会有四个子节点providers, consumers, routers, configurators

其中dubbo、接口、providers都是持久化节点，只有url是临时节点。当会话消失(服务器断开与zookeeper的连接)，对应的临时节点会被删除。

dubbo-registry-zookeeper模块中, ZookeeperRegistry 类继承自 FailbackRegistry.
构造方法中通过 ZookeeperTransporter.connect(url)连接到zookeeper.

1.服务提供者进行服务暴露时,会注册到注册中心注册自己
2.父类 FailbackRegistry 失败重试
3.服务消费者进行服务引用时,会注册自己并且订阅 所有服务提供者.消费者会订阅接口下的 providers 的所有子节点。一旦 providers 下的子节点发生改变，就会通知消息给消费者。

父类 FailbackRegistry 支持失败重试
起一个定时任务,遍历 失败发起注册失败的 URL 集合,向zk注册(创建节点)


dubbo-remoting-zookeeper模块中,提供zk传输的支持
ZookeeperTransporter提供了 ZkClient 和 Curator 两种实现.


消费者引用服务 或 提供者暴露服务时,会往zookeeper上注册节点
主要做了以下几步： 
1. 记录注册注册地址 
2. 注册节点到zookeeper上 
3. 捕捉错误信息，出错则记录下来，等待定期器去重新执行

### 发布
provider注册
RegistryProtocol.export()中
1.获得注册中心对象,启动并连接zk

2.向注册中心订阅服务消费者

3.向注册中心注册服务提供者(自己),其实就是在zk中创建该提供者的持久化节点 /dubbo/com.alibaba.dubbo.demo.DemoService/providers/...
然后注册监听器

AbstractRegistry构造方法中
加载本地配置文件 dubbo-registry-demo-provider-127.0.0.1:2181.cache

FallbackRegistry构造方法中
创建重试定时任务并启动

ZookeeperRegistry构造方法中
连接zk
zkClient.addStateListener().该监听器在重连时,调用 recover() 恢复方法

### 订阅
消费者订阅 和 生产者发布 类似

### 通知
notify()时会将zk节点信息写入到缓存,根据配置的策略选择直接保存文件还是用1个线程异步保存文件.