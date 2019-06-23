## zk idea 代码阅读环境搭建
采用最新的 zookeeper release 版本 3.4.13
[zk发行版下载地址](http://archive.apache.org/dist/zookeeper/)

从 github 上 clone 该版本的代码.

### 1. ant 构建 zk 
build.xml 搜索 ant-eclipse-1.0.bin.tar.bz2, 1290行修改
```xml
<get src="http://ufpr.dl.sourceforge.net/project/ant-eclipse/ant-eclipse/1.0/ant-eclipse-1.0.bin.tar.bz2"
dest="${src.dir}/java/ant-eclipse-1.0.bin.tar.bz2" usetimestamp="false" />
```
执行 ant eclipse

### 2. idea 导入这个 eclipse 项目即可 
File -> New -> import project from exist source -> 选eclipse

### 3. zk 单机伪分布式集群搭建
1. 配置 zk.cfg 配置文件

```properties
## zk 单机多节点配置
# The number of milliseconds of each tick
# 默认值 3000ms,用于表示 zk 中最小时间单元很多时间间隔都是使用 ticketTime 的倍数来表示.
tickTime=2000

# The number of ticks that the initial 
# synchronization phase can take
# 默认值 10,即表示 ticketTime 值的10倍.表示 leader 服务器等待 follower 启动 并 完成数据同步的最大时间.
initLimit=10

# The number of ticks that can pass between 
# sending a request and getting an acknowledgement
# 默认值 5,follower 服务器与 leader 之间进行心跳检测的最大延时时间
syncLimit=5

# the directory where the snapshot is stored.
# dataDir : zk 保存数据的目录.默认情况下,zk 将 事务日志文件 也保存在该目录下.
dataDir=D:\\0zc\\my\\02-data-cluster\\Server_A
# 单独配置 保存事务日志 的目录
dataLogDir=D:\\0zc\\my\\02zk-log-cluster\\Server_A

# the port at which the clients will connect
# clientPort : 客户端连接 zk 服务器 的端口,zk 会监听这个端口,接受客户端的访问请求.
# 若多个节点都部署在同一台机器上,clientPort不能重复.如配置成 Server_A 的 clientPort=2181, Server_B 的 clientPort=2182,依次递增
clientPort=2181

# server.A=B:C:D;
# 其中 A 是一个myid数字,表示这个是第几号服务器
# B 是这个服务器的 ip 地址
# C 与 leader 间通信 与 数据同步 的端口
# D 用于选举通信的端口
#server.A=B       :C    :D
server.1=localhost:33331:33341
server.2=localhost:33332:33342
server.3=localhost:33333:33343
```
修改该配置文件,依次配置 Server_B 和 Server_C.

2. 建立 myid 文件

3. idea中运行 zk Server 
运行 QuorumPeerMain 类,启动时在 Run/Debug Configurations -> Configuration -> Program arguments 中 指定zk的配置文件,如`D:\soft\zookeeper-3.3.3-cluster\Server_A\zookeeper-3.3.3\conf\zoo.cfg`
相当于把它作为集群中的 A 服务器节点,依次类推配置 Server_B 和 Server_C