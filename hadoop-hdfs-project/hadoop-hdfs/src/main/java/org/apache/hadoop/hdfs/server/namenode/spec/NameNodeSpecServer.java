package org.apache.hadoop.hdfs.server.namenode.spec;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class NameNodeSpecServer {
  private static final Log LOG = NameNode.LOG;
  private static final Log stateChangeLog = NameNode.stateChangeLog;

  private final NameNode nn;
  private final FSNamesystem namesystem;
  private final NamenodeProtocols rpcServer;
  private final FSDirectory fsDirectory;

  public NameNodeSpecServer(Configuration conf, NameNode nn) {
    this.nn = nn;
    this.rpcServer = nn.getRpcServer();
    this.namesystem = nn.getNamesystem();
    this.fsDirectory = this.namesystem.getFSDirectory();
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
   * @param str marshaled parameters
   * @return marshaled return value
   */
  public byte[] replicaUpcall(long opnum, byte[] str) {
    try {
      ReplicaUpcall.Request req = ReplicaUpcall.Request.parseFrom(str);
      switch (req.getOp()) {
        case LS:

          break;

        case MKDIR:
          boolean result = this.rpcServer.mkdirs(req.getSrc(), new FsPermission((short) req.getMasked()), req.getCreateparent());
          break;

        case RM:

          break;
          default:
            LOG.error("unknown op");
            return new byte[0];
      }

    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    } catch (UnresolvedLinkException e) {
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (NSQuotaExceededException e) {
      e.printStackTrace();
    } catch (ParentNotDirectoryException e) {
      e.printStackTrace();
    } catch (SnapshotAccessControlException e) {
      e.printStackTrace();
    } catch (AccessControlException e) {
      e.printStackTrace();
    } catch (FileAlreadyExistsException e) {
      e.printStackTrace();
    } catch (SafeModeException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new byte[0];
  }

  /**
   * rollback by undoing the logs, editlog them (no need to logsync either)
   * @param current
   * @param to
   * @param opMap
   */
  public void rollbackUpcall(long current, long to, Map<Long, String> opMap) {

  }

  /**
   * delete undo log records from begining to opnum
   * write to
   * @param commitOpnum
   */
  public void commitUpcall(long commitOpnum) {

  }

}
