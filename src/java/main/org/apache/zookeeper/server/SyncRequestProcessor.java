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

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 此 RequestProcessor将请求记录到磁盘。它批量处理有效执行io的请求。在将日志同步到磁盘之前，请求不会传递到下一个RequestProcessor
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();// 请求队列
    private final RequestProcessor nextProcessor;

    private Thread snapInProcess = null;// 快照处理线程
    volatile private boolean running;

    /** 等待被刷新到磁盘的请求队列
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random(System.nanoTime());
    /**
     * The number of log entries to log before starting a snapshot
     */
    private static int snapCount = ZooKeeperServer.getSnapCount();
    
    /** 滚动日志之前 的 日志条数，随机数
     * The number of log entries before rolling the log, number
     * is chosen randomly
     */
    private static int randRoll;

    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }
    
    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
        randRoll = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }
    
    /**
     * Sets the value of randRoll. This method 
     * is here to avoid a findbugs warning for
     * setting a static variable in an instance
     * method. 
     * 
     * @param roll
     */
    private static void setRandRoll(int roll) {
        randRoll = roll;
    }

    @Override
    public void run() {
        try {
            int logCount = 0;// 事务日志记录计数,用于判断记录 n次事务日志后 是否需要进行 snapshot

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            setRandRoll(r.nextInt(snapCount/2));// 设置滚动日志的随机数,该随机数用于防止所有servers在同一时刻进行snapshot操作
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) {// 没有需要刷新到磁盘的请求
                    si = queuedRequests.take();// 从请求队列中取出一个请求，若队列为空会阻塞住
                } else {// 有需要刷新到磁盘的请求
                    si = queuedRequests.poll();// 从请求队列中取出一个请求，若队列为空，则返回空，不会阻塞
                    if (si == null) {// 取出的请求为空(说明队列取完了 将所有要刷盘的请求队列刷盘)
                        flush(toFlush);// 刷新到磁盘
                        continue;// 不走后面的逻辑,进行下一次 while循环
                    }
                }
                if (si == requestOfDeath) {
                    break;
                }
                if (si != null) {
                    // track the number of records written to the log
                    if (zks.getZKDatabase().append(si)) {// 将请求添加至日志文件，只有事务性请求才会返回true
                        logCount++;// 事务日志写入计数 加 1
                        if (logCount > (snapCount / 2 + randRoll)) {// 事务日志记录到一定次数后,进行 滚动日志 和 snapshot
                            setRandRoll(r.nextInt(snapCount/2));// 重置 滚动日志 随机数
                            // roll the log 滚动日志,创建新的日志文件
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {// 正在进行快照,就不处理了,那就等下一次 while循环再看看吧
                                LOG.warn("Too busy to snap, skipping");
                            } else {// 未启动快照处理线程(每次都新起一个线程进行 snapshot吗 ?)
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                        public void run() {
                                            try {
                                                zks.takeSnapshot();// 启动线程处理快照
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();// 启动线程
                            }
                            logCount = 0;// 重置计数
                        }
                    } else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    toFlush.add(si);// 将请求添加至 toFlush队列
                    if (toFlush.size() > 1000) {// 队列大小 大于1000，直接刷新到磁盘
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }
    // 刷盘
    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())
            return;

        zks.getZKDatabase().commit();// 事务日志刷盘
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();// 取出请求,调用nextProcessor
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
