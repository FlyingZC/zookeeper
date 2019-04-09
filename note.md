##jute序列化
org.apache.jute包
org.apache.jute.BinaryInputArchiveTest
main方法 测试类

##持久化
org.apache.zookeeper.server.persistence包
　　· TxnLog，接口类型，读取事务性日志的接口。

　　· FileTxnLog，实现TxnLog接口，添加了访问该事务性日志的API。

　　· Snapshot，接口类型，持久层快照接口。

　　· FileSnap，实现Snapshot接口，负责存储、序列化、反序列化、访问快照。

　　· FileTxnSnapLog，封装了TxnLog和SnapShot。

　　· Util，工具类，提供持久化所需的API。

###Watcher
Watcher

