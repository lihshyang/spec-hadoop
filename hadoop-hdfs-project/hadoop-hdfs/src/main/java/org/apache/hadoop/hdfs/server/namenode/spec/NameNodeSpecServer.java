package org.apache.hadoop.hdfs.server.namenode.spec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class NameNodeSpecServer {
  private static final Log LOG = NameNode.LOG;
  private static final Log stateChangeLog = NameNode.stateChangeLog;

  private final NameNode nn;
  private final FSNamesystem namesystem;
  private final NamenodeProtocols rpcServer;
  private final FSDirectory fsDirectory;

  private LinkedList<UpcallLog> upcallLogs;

  public NameNodeSpecServer(Configuration conf, NameNode nn) {
    this.nn = nn;
    this.rpcServer = nn.getRpcServer();
    this.namesystem = nn.getNamesystem();
    this.fsDirectory = this.namesystem.getFSDirectory();
    UpcallLog.setNn(nn);
    upcallLogs = new LinkedList<UpcallLog>();
  }

  public void start() {

  }

  public void stop() {

  }

  /**
   * for read operations: return the result
   * for write operations:
   *   create a undo log record and append to a undo log,
   *   apply the op to the namespace, editlog it (no need to logsync)
   *   return the result
   * @param opnum operation number
   * @param param marshaled parameters
   * @return marshaled return value
   */
  public byte[] replicaUpcall(long opnum, byte[] param) {
    ReplicaUpcall.Request req;
    UpcallLog log;

    try {
      req = ReplicaUpcall.Request.parseFrom(param);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return ReplicaUpcall.Reply.newBuilder().setException("InvalidProtocolBuffer").build().toByteArray();
    }

    switch (req.getOp()) {

      case LS:
        try {
          DirectoryListing result = rpcServer.getListing(req.getSrc(), req.getStartAfter().toByteArray(), req.getNeedLocation());
          for (HdfsFileStatus status: result.getPartialListing()) {
            status.voidTimestamps();
          }
          return ReplicaUpcall.Reply.newBuilder().setDirectoryListing(ByteString.
              copyFrom(SerializationUtils.serialize(result))).build().toByteArray(); // TODO need to check if serializing works
        } catch (IOException e) {
          e.printStackTrace();
          return ReplicaUpcall.Reply.newBuilder().setException(e.getMessage()).build().toByteArray();
        }

      case MKDIR:
        log = new UpcallLog(opnum);
        upcallLogs.add(log);
        try {
          UpcallLog.currentOpLog = log;
          boolean result = rpcServer.mkdirs(req.getSrc(), new FsPermission((short) req.getMasked()), req.getCreateParent());
          return ReplicaUpcall.Reply.newBuilder().setSuccess(result).build().toByteArray();
        } catch (IOException e) {
          e.printStackTrace();
          return ReplicaUpcall.Reply.newBuilder().setException(e.getMessage()).build().toByteArray();
        } finally {
          UpcallLog.currentOpLog = null;
        }

      case RM:
        log = new UpcallLog(opnum);
        upcallLogs.add(log);
        try {
          UpcallLog.currentOpLog = log;
          boolean result = rpcServer.delete(req.getSrc(), req.getRecursive());
          return ReplicaUpcall.Reply.newBuilder().setSuccess(result).build().toByteArray();
        } catch (IOException e) {
          e.printStackTrace();
          return ReplicaUpcall.Reply.newBuilder().setException(e.getMessage()).build().toByteArray();
        } finally {
          UpcallLog.currentOpLog = null;
        }

      default:
        LOG.error("unknown op");
        return ReplicaUpcall.Reply.newBuilder().setException("unknown op").build().toByteArray();
    }

  }

  /**
   * rollback by undoing the logs, editlog them (no need to logsync either)
   * @param current must be the latest opnum, right?...
   * @param to
   */
  public void rollbackUpcall(long current, long to) {
    Iterator<UpcallLog> iter = upcallLogs.descendingIterator();
    while (iter.hasNext()) {
      UpcallLog l = iter.next();
      if (l.getOpNum() >= to) {
        l.undo();
        iter.remove();
      } else {
        break;
      }
    }
  }

  /**
   * delete undo log records from begining to opnum
   * write to
   * @param commitOpnum
   */
  public void commitUpcall(long commitOpnum) {
    Iterator<UpcallLog> iter = upcallLogs.iterator();
    while (iter.hasNext()) {
      UpcallLog l = iter.next();
      if (l.getOpNum() <= commitOpnum) {
        iter.remove();
      } else {
        break;
      }
    }
  }
}
