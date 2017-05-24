package org.apache.hadoop.hdfs.server.namenode.spec;


import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;

import static org.apache.hadoop.util.Time.now;

/**
 * Created by aolx on 5/23/17.
 */
public class UpcallLog {
    private static NameNode nn;
    private long opNum;
    private Queue<LogRecord> records;

    public void init(NameNode nn) {
        UpcallLog.nn = nn;
    }

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
                    nn.getNamesystem().getFSDirectory().writeLock();
                    nn.getNamesystem().getFSDirectory().unprotectedDelete(dir, now());
                    nn.getNamesystem().getFSDirectory().writeUnlock();
                } catch (UnresolvedLinkException e) {
                    e.printStackTrace();
                    return false;
                } catch (QuotaExceededException e) {
                    e.printStackTrace();
                    return false;
                } catch (SnapshotAccessControlException e) {
                    e.printStackTrace();
                    return false;
                }
                return true;
            }
        }
    }
}
