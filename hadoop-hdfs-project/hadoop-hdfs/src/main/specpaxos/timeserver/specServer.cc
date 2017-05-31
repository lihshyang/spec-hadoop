#include "timeserver/specServer.h"

HdfsServer::HdfsServer(CommitUpcall_t commitFunc1, ReplicaUpcall_t replicaFunc1, RollbackUpcall_t rollbackFunc1)
{
    commitFunc = commitFunc1;
    replicaFunc = replicaFunc1;
    rollbackFunc = rollbackFunc1;
}
HdfsServer::HdfsServer() {}

HdfsServer::~HdfsServer() { }

void
HdfsServer::ReplicaUpcall(opnum_t opnum,
                               const string &str1,
                               string &str2)
{
    char buf[str2.length()]; //allocate more than str2.length() ??
    strcpy(buf, str2.c_str());
    replicaFunc(opnum, str1.c_str(), buf);
    str2 = buf;
    Debug("Received Upcall: " FMT_OPNUM ", %s", opnum, str1.c_str());

}

void
HdfsServer::RollbackUpcall(opnum_t current,
                                opnum_t to,
                                const std::map<opnum_t, string> &opMap)
{
    rollbackFunc(current, to);
    Debug("Received Rollback Upcall: " FMT_OPNUM ", " FMT_OPNUM, current, to);

}


void
HdfsServer::CommitUpcall(opnum_t commitOpnum)
{
    Debug("Received Commit Upcall: " FMT_OPNUM, commitOpnum);
    commitFunc(commitOpnum);
}


void run(const char* configDir, CommitUpcall_t commitFunc, ReplicaUpcall_t replicaFunc, RollbackUpcall_t rollbackFunc) {
    int index = -1;
    string configPath = configDir;
    enum {
        PROTO_UNKNOWN,
        PROTO_VR,
        PROTO_SPEC,
        PROTO_FAST
    } proto = PROTO_UNKNOWN;
    proto = PROTO_SPEC;

    // Load configuration
    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
            configPath);
    }
    specpaxos::Configuration config(configStream);

    if (index >= config.n) {
        fprintf(stderr, "replica index %d is out of bounds; "
            "only %d replicas defined\n", index, config.n);
    }

    UDPTransport transport(0.0, 0.0, 0);

    specpaxos::Replica *replica;
    HdfsServer server(commitFunc, replicaFunc, rollbackFunc);
    //server.CommitUpcall(233333);
    //const string str1 = "str1";
    //string str2 = "str2";
    //server.ReplicaUpcall(233, str1, str2);
    //cout << "after replicaUpcall str2: " << str2 << endl;
    replica = new specpaxos::spec::SpecReplica(
              config, index, true, &transport, &server);

    (void)replica;              // silence warning
    transport.Run();




	//commitFunc(arg);
}
