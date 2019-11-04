/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.util.ArrayList;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /** 请求队列(不一定是事务请求).queuedRequests 中的请求是在 proposalRequestProcessor.processRequest()中加入进来的.这些请求当时还没有进行 proposal和 commit包的处理
     * Requests that we are holding until the commit comes in.
     */
    LinkedList<Request> queuedRequests = new LinkedList<Request>();

    /** committedRequests中的请求时在执行完 proposal和 commit包 后加入进来的 (需要被应用到内存数据库的 事务请求)
     * Requests that have been committed.
     */
    LinkedList<Request> committedRequests = new LinkedList<Request>();

    RequestProcessor nextProcessor;
    ArrayList<Request> toProcess = new ArrayList<Request>();// 所有非事务请求添加到 toProcess队列,用于交给下个processor处理

    /** 对于leader是false,对于learner是true.因为learner端sync请求需要等待leader回复,而leader端本身则不需要
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id,
            boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    volatile boolean finished = false;

    @Override
    public void run() {
        try {
            Request nextPending = null;// 下一个等待处理的事务请求
            while (!finished) {
                int len = toProcess.size();
                for (int i = 0; i < len; i++) {//  遍历 toProcess队列(非事务请求或者已经提交的事务请求),交给下一个处理器处理，清空队列
                    nextProcessor.processRequest(toProcess.get(i));
                }
                toProcess.clear();
                synchronized (this) {// 同步监视器是当前类的对象
                    if ((queuedRequests.size() == 0 || nextPending != null)
                            && committedRequests.size() == 0) {// 1.请求队列为空 且 commit队列也为空; 2.[请求队列不为空但是有标记要处理的请求] 且 commit队列为空
                        wait();// 等待 commit请求进来
                        continue;
                    }
                    // First check and see if the commit came in for the pending
                    // request
                    if ((queuedRequests.size() == 0 || nextPending != null)
                            && committedRequests.size() > 0) {// 有 commit请求进来. 1.请求队列为空 且 commit队列不为空; 2.[请求队列不为空但是有 标记要处理的请求] 且 commit队列不为空
                        Request r = committedRequests.remove();// 取出 commit请求
                        /*
                         * We match with nextPending so that we can move to the
                         * next request when it is committed. We also want to
                         * use nextPending because it has the cnxn member set
                         * properly.
                         */
                        if (nextPending != null
                                && nextPending.sessionId == r.sessionId
                                && nextPending.cxid == r.cxid) {// 判断 取出来的 request和 nextPending 是否匹配
                            // we want to send our version of the request.
                            // the pointer to the connection in the request
                            nextPending.hdr = r.hdr;
                            nextPending.txn = r.txn;
                            nextPending.zxid = r.zxid;
                            toProcess.add(nextPending);// 匹配的话,进入toProcess队列,nextPending置空
                            nextPending = null;
                        } else {
                            // this request came from someone else so just
                            // send the commit packet
                            toProcess.add(r);
                        }
                    }
                }

                // We haven't matched the pending requests, so go back to
                // waiting
                if (nextPending != null) {// 若 nextPending非空，就不用再去遍历请求队列，找到下一个事务请求(不执行下面的逻辑)
                    continue;
                }
                // 从 queuedRequests 中找到第一个事务请求给 nextPending 赋值
                synchronized (this) {// 加锁
                    // Process the next requests in the queuedRequests
                    while (nextPending == null && queuedRequests.size() > 0) {// 遍历取queuedRequests中的request,非事务请求添加到toProcess中,事务请求赋值给nextPending(退出循环)
                        Request request = queuedRequests.remove();
                        switch (request.type) {
                        case OpCode.create:
                        case OpCode.delete:
                        case OpCode.setData:
                        case OpCode.multi:
                        case OpCode.setACL:
                        case OpCode.createSession:
                        case OpCode.closeSession:
                            nextPending = request;// 事务请求
                            break;
                        case OpCode.sync:
                            if (matchSyncs) {
                                nextPending = request;
                            } else {
                                toProcess.add(request);
                            }
                            break;
                        default:
                            toProcess.add(request);// 所有非事务请求添加到 toProcess队列,用于交给下个processor处理
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted exception while waiting", e);
        } catch (Throwable e) {
            LOG.error("Unexpected exception causing CommitProcessor to exit", e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    synchronized public void commit(Request request) {// 事务请求提交
        if (!finished) {
            if (request == null) {
                LOG.warn("Committed a null!",
                         new Exception("committing a null! "));
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing request:: " + request);
            }
            committedRequests.add(request);// 请求 添加到 已提交队列
            notifyAll();
        }
    }
    // leader 和 所有 follower 在 ProposalRequestProcessor 中 都会调用该方法
    synchronized public void processRequest(Request request) {
        // request.addRQRec(">commit");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        
        if (!finished) {
            queuedRequests.add(request);// 将请求 添加到 请求队列
            notifyAll();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        synchronized (this) {
            finished = true;
            queuedRequests.clear();
            notifyAll();
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
