### processors
* PrepRequestProcessor
通常是请求处理链的第一个处理器.
识别客户端请求是否是事务请求.对于事务请求,做预处理,诸如 创建请求事务头,事务体,会话检查,ACL检查 和 版本检查 等.

* ProposalRequestProcessor
leader 服务器的`事务投票处理器`,也是 leader 服务器事务处理流程的发起者.
对于 非事务请求,它会直接将请求流转到 CommitProcessor 处理器,不再做其他处理.
对于 事务请求,除了将请求交给CommitProcessor处理器外,还会根据请求类型创建对应的 Proposal 提议,并 发给所有 Follower 来发起一次集群内的事务投票.它还会将事务请求交给 SyncRequestProcessor 进行事务日志的记录.

* SyncRequestProcessor 
事务日志记录处理器,将事务请求记录到事务日志文件中,同时还会触发zk进行数据快照.
发送Sync请求的处理器.将请求记录到磁盘.它批量处理有效执行io的请求.在将日志同步到磁盘之前,请求不会传递到下一个 Processor.
维护了一个处理请求的队列,其用于存放请求;
维护了一个处理快照的线程,用于处理快照;
维护了一个等待被刷新到磁盘的请求队列.
将事务性请求刷新到磁盘,并且对请求进行快照处理.

* AckRequestProcessor
leader 独有的处理器,它负责在 SyncRequestProcessor 处理器完成事务日志记录后,向 Proposal 的投票收集器发送 ACK 反馈,以通知投票收集器当前服务器已经完成了对该 Proposal 的事务日志记录.
将前一阶段的请求作为 ACK 转发给 Leader.

* CommitProcessor
事务提交处理器.
对于非事务请求,该处理器直接将其交给下一个处理器进行处理.
对于事务请求,它会等待集群内 针对 Proposal 的投票,直到该 Proposal 可被提交.

* ToBeAppliedRequestProcessor
维护 toBeApplied 队列,专门存储那些已经被 CommitProcessor 处理过的可被提交的 Proposal.
它将这些请求逐个交付给 FinalRequestProcessor 处理器进行处理,等待 FinalRequestProcessor 处理器处理完后,再将其从toBeApplied 队列中移除.
下个处理器必须是 FinalRequestProcessor 并且 FinalRequestProcessor 必须同步处理请求.

* FinalRequestProcessor
通常是请求处理链的最后一个处理器.
创建客户端请求的响应;针对事务请求,它还会负责`将事务应用到内存数据库`中.

* FollowerRequestProcessor
它是 follower 的第一个请求处理器.用于识别当前请求是否是事务请求.
若是事务请求,转发给 leader 服务器.leader 在收到这个事务请求后,就会将其提交到请求处理器链,按照正常事务请求进行处理.

* ObserverRequestProcessor
同 FollowerRequestProcessor 一样,将事务请求转发给 Leader.

* SendAckRequestProcessor
follower 独有,发送 ACK 请求的处理器.
在 follower 完成事务日志记录后,会向 leader 服务器发送 ACK 消息表面自身完成了事务日志的记录工作.
它和 leader 的 AckRequestProcessor 的区别:
AckRequestProcessor 处理器和 leader 服务器在同一个服务器上,它的 ACK 反馈仅仅是一个本地操作.
SendAckRequestProcessor 处理器在 follower 服务器上,需要通过 ACK 消息向 leader 服务器进行反馈.

### processor链对事务请求的处理
![processor链对事务请求的处理](../assets/zk-processor.jpg)