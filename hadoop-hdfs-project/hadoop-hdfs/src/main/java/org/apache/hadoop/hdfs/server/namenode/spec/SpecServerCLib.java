package org.apache.hadoop.hdfs.server.namenode.spec;

/**
 * Created by liyinan on 30/5/2017.
 */

import com.sun.jna.Library;
import com.sun.jna.Callback;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public interface SpecServerCLib extends Library {
  interface CommitUpcall_t extends Callback {
    void invoke(long opnum);
  }

  interface ReplicaUpcall_t extends Callback {
    void invoke(long opnum, Pointer str1, Pointer str2);
  }

  interface RollbackUpcall_t extends Callback {
    void invoke(long current, long to);
  }
  void newClientPtr(String configPath, PointerByReference ppClient);
  void test1();
  void runClient(Pointer clientPtr, String req, PointerByReference reply);
  void run(String configPath, int index, CommitUpcall_t commitUpcall, ReplicaUpcall_t replicaUpcall, RollbackUpcall_t rollbackUpcall);
}

