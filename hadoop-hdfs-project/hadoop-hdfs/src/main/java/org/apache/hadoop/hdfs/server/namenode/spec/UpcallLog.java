package org.apache.hadoop.hdfs.server.namenode.spec;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.util.Time.now;

/**
 * Created by aolx on 5/23/17.
 */
public class UpcallLog {
    public static final Log LOG = LogFactory.getLog(UpcallLog.class.getName());

    private static NameNode nn;

    public long getOpNum() {
        return opNum;
    }

    private long opNum;
    private Queue<LogRecord> records;

    static volatile ReentrantLock upcallLogLock = new ReentrantLock();
    static volatile UpcallLog currentOpLog;

    public static void setNn(NameNode nn) {
        UpcallLog.nn = nn;
    }

    public static ReentrantLock getUpcallLogLock() {
        return upcallLogLock;
    }

    public static UpcallLog getCurrentOpLog() { return currentOpLog; }

    public UpcallLog(long opNum) {
        this.opNum = opNum;
        this.records = Collections.asLifoQueue(new LinkedList<LogRecord>());
    }

    public void append(LogRecord r) {
        records.add(r);
    }

    public boolean undo() {
        for (LogRecord r: records) {
            if (!r.undo()) {
                LOG.warn("undoing: " + r + " unsuccessful");
                return false;
            }
        }
        return true;
    }

    public static abstract class LogRecord {
        abstract boolean undo();

        public static class MkdirRecord extends LogRecord {
            String dir;

            public MkdirRecord(String dir) {
                this.dir = dir;
            }

            @Override
            boolean undo() {
                try {
                    return nn.getRpcServer().delete(dir, false);
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            @Override
            public String toString() {
                return "mkdir: " + dir;
            }
        }

        public static class DeleteRecord extends LogRecord {
            String dir;
            FsPermission permission;

            /**
             * now only support rmdir semantics
             */
            public DeleteRecord(String dir, FsPermission permission) {
                this.dir = dir;
                this.permission = permission;
            }

            @Override
            boolean undo() {
                try {
                    return nn.getRpcServer().mkdirs(dir, permission, false);
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
    }
}
