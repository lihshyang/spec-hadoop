#include "lib/configuration.h"
#include "common/replica.h"
#include "lib/udptransport.h"
#include "spec/replica.h"
#include "vr/replica.h"
#include "fastpaxos/replica.h"
#include <iostream>
#include <string>

using namespace std;
typedef void (*CommitUpcall_t)(long op);
typedef void (*ReplicaUpcall_t)(long opnum, const char* str1, char str2[]);
typedef void (*RollbackUpcall_t)(long current, long to);

class HdfsServer : public specpaxos::AppReplica
{
public:
    HdfsServer(CommitUpcall_t commitFunc, ReplicaUpcall_t replicaFunc, RollbackUpcall_t rollbackFunc);
    HdfsServer();
    ~HdfsServer();

    void ReplicaUpcall(opnum_t opnum, const string &str1, string &str2);
    void RollbackUpcall(opnum_t current, opnum_t to, const std::map<opnum_t, string> &opMap);
    void CommitUpcall(opnum_t op);
private:
    CommitUpcall_t commitFunc;
    ReplicaUpcall_t replicaFunc;
    RollbackUpcall_t rollbackFunc;
};


extern "C" void run(CommitUpcall_t commitFunc, ReplicaUpcall_t replicaFunc, RollbackUpcall_t rollbackFunc);
