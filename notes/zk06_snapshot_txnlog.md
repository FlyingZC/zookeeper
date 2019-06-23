## ZKDataBase结构
ZKDataBase(zk内存数据库)
    -- sessionWithTimeouts(zk所有会话  会话超时时间记录器)
    -- DataTree存储
    -- 事务日志

ZKDatabase会定时向磁盘dump快照数据,
在zk启动时通过磁盘上的事务日志 和 快照文件 恢复 一个完整的内存数据库

## 事务日志
dataLogDir

文件大小都是64MB

文件名 log.xxx
xxx为事务id : zxid, 且 `为写入该事务文件第一条事务记录的zxid`.

zxid由两部分组成: 高32位 为 当前leader周期(epoch), 低32位 为 真正的操作序列号.

LogFormatter 可以查看该文件

### 事务日志操作
1. 滚动日志
FileTxnLog.rollLog()
将当前日志文件 滚动为新文件.
其实就是将 logStream置为null,那么要写入日志时就会新创建一个日志文件用于写入.

2. 写事务日志
FileTxnLog.append(TxnHeader hdr, Record txn)

若logStream == null, 代表要新创建一个事务日志文件.则根据根据事务头里的 zxid创建事务文件名.
创建事务日志文件头信息(魔数,版本,dbId),序列化成fileheader,刷新到磁盘.

填充文件.

将 事务头 和 事务数据 序列化成 Byte Buffer,生成一个验证算法,将序列化的事务记录写入OutputArchive.

### 事务日志写入时机
SyncRequestProcessor.run()里面调用

### 事务日志文件的磁盘空间预分配
事务日志文件大小默认均为64MB,文件大小是预分配的,用"0"填充的,每个事务日志都会及时记录到里面,相当于修改这部分文件.

这样做的好处:

对于客户端的每一次事务操作,zk都会将其写入事务日志文件中.

事务日志的写入性能直接决定了zk事务请求的响应.

事务写入近似可以被看做是一个磁盘I/O的过程.

文件的不断追加写入操作会触发底层磁盘I/O为文件开辟新的磁盘块,即磁盘seek.

为了避免磁盘Seek的频率,提高磁盘I/O效率.zk创建事务日志的时候就会进行文件空间预分配,默认64MB.

一旦已分配的文件空间不足4kb,会再次预分配,以避免随着每次事务的写入过程中文件大小增长带来的seek开销,直至创建新的事务日志.

事务日志"预分配"的大小可以通过系统属性 `zookeeper.preAllocSize` 来设置.

根据序列化产生的字节数组来计算Checksum,保证事务文件的完整性 和 准确性.

写入流 -> 最后将流刷盘.

## snapshot
snapshot 用于记录zk服务器上某个时刻的`全量内存数据`,并将其写入到指定的磁盘文件中.

dataDir
文件名 snapshot.xxxx
每个快照数据文件中的所有内容都是有效的,不会预分配.

SnapshotFormatter 可以查看该文件,该类仅输出每个数据节点的元信息,不输出节点数据内容.

事务操作,zk会记录到事务日志,并且将数据变更应用到内存数据库中.

zk会在进行若干次事务日志记录后,将内存数据库的全量数据 Dump 到本地文件中,即 数据快照.
可`配置zk在snapCount次事务日志记录后`进行一个数据快照.

### 何时进行快照
逻辑在 SyncRequestProcessor 类中.
logCount > (snapCount / 2 + randRool)
如 snapCount 默认配置为`10,0000` ,zk会在 5,0000 - 10,0000 次事务后进行一次数据快照. 

随机数避免集群中所有机器在同一时刻进行数据快照.

snapshot 操作单独在一个异步线程处理,将所有`DataTree`和`会话信息`保存到本地磁盘中.

根据当前已提交的 最大ZXID 来生成数据快照文件名.

数据序列化: 
文件头: 魔数, 版本号, dbid
会话信息, DataTree, Checksum

## 初始化时将 snapshot 和 txnlog 应用到内存数据库
zk 启动时,会根据 snapshot 和 txnlog 恢复内存数据库.

默认会获取磁盘上最新的100个快照文件,但是时逐个解析的,只有最新的文件校验不通过时,才会解析之前的(相当于 `使用最新并可用的那个快照文件`).

根据快照文件名 获取 zxid 即 zxid_for_snap.

处理事务日志.`从事务日志中获取所有比 zxid_for_snap 大的事务操作`.

将事务应用到 ZKDatabase 和 sessionWithTimeouts 中.

zk每应用一个事务日志,会回调 PlayBackListener 监听器,将这个事务操作转换成Proposal,保存到 ZKDatabase.committedLog 中,以便启动选举出 leader 后 follower 同步 leader 的数据.

zxid : zookeeper事务id
epoch : 标识当前leader周期,每次选举产生一个新的leader服务器后,都会生产一个新的epoch.

## snapshot + txnlog 如何保证全量数据
一个全量的内存数据库.它包含
1. 一个最新的可用的快照(假设它的文件名为zxid_for_snap)
2. 事务id大于等于该 zxid_for_snap 的所有事务日志
3. 事务id小于该 zxid_for_snap 的那个最新的事务日志(比如在往这个事务文件里写事务日志,写着写着进行了快照操作,但是由于这个事务日志还没写满,所以会接着往这里面写事务日志)

## 配置项 zookeeper.snapCount
zookeeper.snapCount 必须 >= 2,若小于2会被置为2,默认不配置则取 10,0000



