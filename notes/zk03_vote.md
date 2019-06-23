### leader选举相关线程
QuorumCnxManager : 管理者,包含发送 和 接收 队列.持有 listener.

QuorumCnxManager.Listener : 监听 并 建立 server 之间的连接.

FastLeaderElection.Messenger.SendWorker
消息生产者.循环 从发送队列 queueSendMap 中取消息(出队)并发送出去.并将该消息存入 lastMessageSent 中.

FastLeaderElection.Messenger.RecvWorker
消息消费者.循环 接收线程从底层 Socket 收到报文后放到 recvQueue 队列中,等待 Messenger 调用 pollRecvQueue 方法获取消息.

### leader选举流程
启动时选举
选举算法的创建之前先创建 QuorumCnxManager,它通过 TCP 协议来进行 leader 选举.
每一对server之间都会保持一个TCP链接.
`zookeeper服务之间都是配置myid大的作为客户端连接,myid小的作为服务器端`.
创建server端的发送线程和接收线程.

投票默认先投自己,投票信息包括(sid,zxid,echo),将投票信息入待发送队列,等待sendWorker线程将投票发送给所有其他server.

下面一直循环操作,直到选出Leader为止.
从接收队列中取出接收到的投票,校验投票信息中的sid和所投票的leader.

将接收到的投票 和 自己的投票 pk.
比较优先级 `epoch > zxid > sid`.
判断投票消息里的 epoch 周期是不是比当前的大,如果大则消息中id对应的服务器就是leader.
若epoch相等则判断zxid,如果消息里的zxid大,则消息中id对应的服务器就是leader.
若前面两个都相等那就比较服务器id,若大,则其就是leader.

更新自己的投票,再向集群中所有机器发出去.

每次投票后,当前服务器都会统计本轮接收到的所有投票(recvset中投票一致数),若有过半机器接收到了相同的投票信息,则认为选出了leader.

变更状态,following 或 leading.

leader挂了选举
其余非Observer服务器都会将自己状态变更为 LOOKING,进入leader选举流程.

![zk-vote
](../assets/zk-vote.jpg)