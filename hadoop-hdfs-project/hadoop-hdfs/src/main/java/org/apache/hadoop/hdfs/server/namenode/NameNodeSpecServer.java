package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class NameNodeSpecServer {
  private static final Log LOG = NameNode.LOG;
  private static final Log stateChangeLog = NameNode.stateChangeLog;

  private final NameNode nn;
  private final FSNamesystem namesystem;

  public NameNodeSpecServer(Configuration conf, NameNode nn) {
    this.nn = nn;
    this.namesystem = nn.getNamesystem();
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
   * @param str1 marshaled parameters
   * @param str2 marshaled return value
   */
  public void ReplicaUpcall(long opnum, String str1, StringBuffer str2) {

  }

  /**
   * rollback by undoing the logs, editlog them (no need to logsync either)
   * @param current
   * @param to
   * @param opMap
   */
  public void RollbackUpcall(long current, long to, Map<Long, String> opMap) {

  }

  /**
   * delete undo log records from begining to opnum
   * write to
   * @param commitOpnum
   */
  public void CommitUpcall(long commitOpnum) {

  }

}
