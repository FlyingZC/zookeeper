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

package org.apache.zookeeper.server.persistence;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 封装了TxnLog和SnapShot的辅助类
 * This is a helper class 
 * above the implementations 
 * of txnlog and snapshot 
 * classes
 */
public class FileTxnSnapLog {
    //the direcotry containing the 
    //the transaction logs 日志文件目录
    private final File dataDir;
    //the directory containing the
    //the snapshot directory 快照文件目录
    private final File snapDir;
    private TxnLog txnLog;// 事务日志
    private SnapShot snapLog;// 快照
    public final static int VERSION = 2;// 版本号
    public final static String version = "version-";
    
    private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);
    
    /** 接收事务应用过程中的回调，在Zookeeper数据恢复后期，会有事务修正过程，此过程会回调PlayBackListener来进行对应的数据修正.每当有一条事务日志应用到内存数据库后,就会调用这个监听器
     * This listener helps
     * the external apis calling
     * restore to gather information
     * while the data is being 
     * restored.
     */
    public interface PlayBackListener {
        void onTxnLoaded(TxnHeader hdr, Record rec);
    }
    
    /**
     * the constructor which takes the datadir and 
     * snapdir.
     * @param dataDir the trasaction directory
     * @param snapDir the snapshot directory
     */
    public FileTxnSnapLog(File dataDir, File snapDir) throws IOException {
        LOG.debug("Opening datadir:{} snapDir:{}", dataDir, snapDir);
        // 在datadir和snapdir下生成version-2目录
        this.dataDir = new File(dataDir, version + VERSION);
        this.snapDir = new File(snapDir, version + VERSION);
        if (!this.dataDir.exists()) {
            if (!this.dataDir.mkdirs()) {
                throw new IOException("Unable to create data directory "
                        + this.dataDir);
            }
        }
        if (!this.dataDir.canWrite()) {
            throw new IOException("Cannot write to data directory " + this.dataDir);
        }

        if (!this.snapDir.exists()) {
            if (!this.snapDir.mkdirs()) {
                throw new IOException("Unable to create snap directory "
                        + this.snapDir);
            }
        }
        if (!this.snapDir.canWrite()) {
            throw new IOException("Cannot write to snap directory " + this.snapDir);
        }

        // check content of transaction log and snapshot dirs if they are two different directories
        // See ZOOKEEPER-2967 for more details
        if(!this.dataDir.getPath().equals(this.snapDir.getPath())){
            checkLogDir();
            checkSnapDir();
        }
        // 创建txnLog和snapLog对象
        txnLog = new FileTxnLog(this.dataDir);
        snapLog = new FileSnap(this.snapDir);
    }

    public void setServerStats(ServerStats serverStats) {
        txnLog.setServerStats(serverStats);
    }

    private void checkLogDir() throws LogDirContentCheckException {
        File[] files = this.dataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isSnapshotFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new LogDirContentCheckException("Log directory has snapshot files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    private void checkSnapDir() throws SnapDirContentCheckException {
        File[] files = this.snapDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isLogFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new SnapDirContentCheckException("Snapshot directory has log files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    /**
     * get the datadir used by this filetxn
     * snap log
     * @return the data dir
     */
    public File getDataDir() {
        return this.dataDir;
    }
    
    /**
     * get the snap dir used by this 
     * filetxn snap log
     * @return the snap dir
     */
    public File getSnapDir() {
        return this.snapDir;
    }
    
    /** 恢复datatree和sessions,从快照 和 事务日志 中读取后 还原服务器数据库
     * this function restores the server 
     * database after reading from the 
     * snapshots and transaction logs
     * @param dt the datatree to be restored
     * @param sessions the sessions to be restored
     * @param listener the playback listener to run on the 
     * database restoration
     * @return the highest zxid restored
     * @throws IOException
     */
    public long restore(DataTree dt, Map<Long, Integer> sessions, 
            PlayBackListener listener) throws IOException {
        snapLog.deserialize(dt, sessions);// 从最后一个有效快照 反序列化 数据树，并返回反序列化的最后一个zxid
        return fastForwardFromEdits(dt, sessions, listener);
    }

    /** 读取事务日志
     * This function will fast forward the server database to have the latest
     * transactions in it.  This is the same as restore, but only reads from
     * the transaction logs and not restores from a snapshot.
     * @param dt the datatree to write transactions to.
     * @param sessions the sessions to be restored.
     * @param listener the playback listener to run on the
     * database transactions.
     * @return the highest zxid restored.
     * @throws IOException
     */
    public long fastForwardFromEdits(DataTree dt, Map<Long, Integer> sessions,
                                     PlayBackListener listener) throws IOException {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        TxnIterator itr = txnLog.read(dt.lastProcessedZxid+1);
        long highestZxid = dt.lastProcessedZxid;
        TxnHeader hdr;
        try {
            while (true) {
                // iterator points to 
                // the first valid txn when initialized
                hdr = itr.getHeader();
                if (hdr == null) {
                    //empty logs 
                    return dt.lastProcessedZxid;
                }
                if (hdr.getZxid() < highestZxid && highestZxid != 0) {
                    LOG.error("{}(higestZxid) > {}(next log) for type {}",
                            new Object[] { highestZxid, hdr.getZxid(),
                                    hdr.getType() });
                } else {
                    highestZxid = hdr.getZxid();
                }
                try {
                    processTransaction(hdr,dt,sessions, itr.getTxn());
                } catch(KeeperException.NoNodeException e) {
                   throw new IOException("Failed to process transaction type: " +
                         hdr.getType() + " error: " + e.getMessage(), e);
                }
                listener.onTxnLoaded(hdr, itr.getTxn()); // 每当成功将一条事务日志应用到内存数据库中后,会回调 PlayBackListener 监听器.它会将这些刚被应用到内存数据库中的事务转存到 ZKDatabase.committedLog 中,以便后面 数据同步使用.
                if (!itr.next()) 
                    break;
            }
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
        return highestZxid;
    }
    
    /**
     * process the transaction on the datatree
     * @param hdr the hdr of the transaction
     * @param dt the datatree to apply transaction to
     * @param sessions the sessions to be restored
     * @param txn the transaction to be applied
     */
    public void processTransaction(TxnHeader hdr,DataTree dt,
            Map<Long, Integer> sessions, Record txn)
        throws KeeperException.NoNodeException {
        ProcessTxnResult rc;
        switch (hdr.getType()) {
        case OpCode.createSession:
            sessions.put(hdr.getClientId(),
                    ((CreateSessionTxn) txn).getTimeOut());
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                        "playLog --- create session in log: 0x"
                                + Long.toHexString(hdr.getClientId())
                                + " with timeout: "
                                + ((CreateSessionTxn) txn).getTimeOut());
            }
            // give dataTree a chance to sync its lastProcessedZxid 让dataTree有机会同步它的lastProcessedZxid
            rc = dt.processTxn(hdr, txn);
            break;
        case OpCode.closeSession:
            sessions.remove(hdr.getClientId());
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                        "playLog --- close session in log: 0x"
                                + Long.toHexString(hdr.getClientId()));
            }
            rc = dt.processTxn(hdr, txn);
            break;
        default:
            rc = dt.processTxn(hdr, txn);
        }

        /**
         * Snapshots are lazily created. So when a snapshot is in progress,
         * there is a chance for later transactions to make into the
         * snapshot. Then when the snapshot is restored, NONODE/NODEEXISTS
         * errors could occur. It should be safe to ignore these.
         */
        if (rc.err != Code.OK.intValue()) {
            LOG.debug("Ignoring processTxn failure hdr:" + hdr.getType()
                    + ", error: " + rc.err + ", path: " + rc.path);
        }
    }

    /**
     * the last logged zxid on the transaction logs
     * @return the last logged zxid
     */
    public long getLastLoggedZxid() {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.getLastLoggedZxid();
    }

    /** 将 datatree和 sessions保存到 snapshot
     * save the datatree and the sessions into a snapshot
     * @param dataTree the datatree to be serialized onto disk
     * @param sessionsWithTimeouts the sesssion timeouts to be
     * serialized onto disk
     * @throws IOException
     */
    public void save(DataTree dataTree,
            ConcurrentHashMap<Long, Integer> sessionsWithTimeouts)
        throws IOException {
        long lastZxid = dataTree.lastProcessedZxid;
        File snapshotFile = new File(snapDir, Util.makeSnapshotName(lastZxid));
        LOG.info("Snapshotting: 0x{} to {}", Long.toHexString(lastZxid),
                snapshotFile);
        snapLog.serialize(dataTree, sessionsWithTimeouts, snapshotFile);
        
    }

    /**
     * truncate the transaction logs the zxid
     * specified
     * @param zxid the zxid to truncate the logs to
     * @return true if able to truncate the log, false if not
     * @throws IOException
     */
    public boolean truncateLog(long zxid) throws IOException {
        // close the existing txnLog and snapLog
        close();

        // truncate it
        FileTxnLog truncLog = new FileTxnLog(dataDir);
        boolean truncated = truncLog.truncate(zxid);
        truncLog.close();

        // re-open the txnLog and snapLog
        // I'd rather just close/reopen this object itself, however that 
        // would have a big impact outside ZKDatabase as there are other
        // objects holding a reference to this object.
        txnLog = new FileTxnLog(dataDir);
        snapLog = new FileSnap(snapDir);

        return truncated;
    }
    
    /**
     * the most recent snapshot in the snapshot
     * directory
     * @return the file that contains the most 
     * recent snapshot
     * @throws IOException
     */
    public File findMostRecentSnapshot() throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findMostRecentSnapshot();
    }
    
    /**
     * the n most recent snapshots
     * @param n the number of recent snapshots
     * @return the list of n most recent snapshots, with
     * the most recent in front
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findNRecentSnapshots(n);
    }

    /** 获取可能包含比给定 zxid更新的事务的事务日志文件。这包括 1.起始 zxid大于给定zxid的日志，以及 2.起始zxid小于给定zxid的最新事务日志。后一个日志文件可能包含超出给定zxid的事务。
     * get the snapshot logs which may contain transactions newer than the given zxid.
     * This includes logs with starting zxid greater than given zxid, as well as the
     * newest transaction log with starting zxid less than given zxid.  The latter log
     * file may contain transactions beyond given zxid.
     * @param zxid the zxid that contains logs greater than
     * zxid
     * @return
     */
    public File[] getSnapshotLogs(long zxid) {
        return FileTxnLog.getLogFiles(dataDir.listFiles(), zxid);
    }

    /** 事务日志写入
     * append the request to the transaction logs
     * @param si the request to be appended
     * returns true iff something appended, otw false 
     * @throws IOException
     */
    public boolean append(Request si) throws IOException {
        return txnLog.append(si.hdr, si.txn);
    }

    /**
     * commit the transaction of logs
     * @throws IOException
     */
    public void commit() throws IOException {
        txnLog.commit();
    }

    /**
     * roll the transaction logs
     * @throws IOException 
     */
    public void rollLog() throws IOException {
        txnLog.rollLog();
    }
    
    /**
     * close the transaction log files
     * @throws IOException
     */
    public void close() throws IOException {
        txnLog.close();
        snapLog.close();
    }

    @SuppressWarnings("serial")
    public static class DatadirException extends IOException {
        public DatadirException(String msg) {
            super(msg);
        }
        public DatadirException(String msg, Exception e) {
            super(msg, e);
        }
    }

    @SuppressWarnings("serial")
    public static class LogDirContentCheckException extends DatadirException {
        public LogDirContentCheckException(String msg) {
            super(msg);
        }
    }

    @SuppressWarnings("serial")
    public static class SnapDirContentCheckException extends DatadirException {
        public SnapDirContentCheckException(String msg) {
            super(msg);
        }
    }
}
