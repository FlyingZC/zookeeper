### zk启动流程
1. 启动类为 QuorumPeerMain
2. 解析 zoo.cfg 配置
3. 创建并启动 DatadirCleanupManager 用于清理过期 snapshot 和 txnlog.
4. 创建 QuorumPeer 实例并启动该线程,用于完成选举.
5. 根据 snapshot 和 txnlog 恢复 内存数据库 ZKDatabase.
![zk启动流程](../assets/zk-start.jpg)