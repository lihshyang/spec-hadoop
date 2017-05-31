// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:

#include "client.h"



void runClient(const char* configDir, const char* req, char** reply) {
    string configPath = configDir;
    hdfsSpec::Client client(configPath);
    const string requestStr = req;
    string replyStr = client.Invoke(requestStr);
    *reply = (char*)malloc(sizeof(char) * (replyStr.length() + 1));
    memset(*reply, 0, sizeof(char) * (replyStr.length() + 1));
    strcpy(*reply, replyStr.c_str());
}
namespace hdfsSpec {
    Client::Client(string configPath)
            : transport(0.0, 0.0, 0) {
        string shardConfigPath = configPath;
        ifstream shardConfigStream(shardConfigPath);
        if (shardConfigStream.fail()) {
            fprintf(stderr, "unable to read configuration file: %s\n",
                    shardConfigPath.c_str());
            exit(0);
        }
        specpaxos::Configuration shardConfig(shardConfigStream);
        shard = new specpaxos::spec::SpecClient(shardConfig, &transport);
        /* Run the transport in a new thread. */
        clientTransport = new thread(&Client::run_client, this);

        Debug("client [%lu] created!");
    }

    Client::~Client() {
        // TODO: Consider killing transport and associated thread.
    }

/* Runs the transport event loop. */
    void
    Client::run_client() {
        transport.Run();
    }

/* Sends BEGIN to a single shard indexed by i. */



/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */

    string Client::Invoke(const string &request) {
        unique_lock<mutex> lk(cv_m);
        transport.Timer(0, [=]() {
            shard->Invoke(request,
                          bind(&Client::invokeCallback,
                               this, 0, placeholders::_1, placeholders::_2));
        });
        cv.wait(lk);
        return replica_reply;
    }

    void
    Client::invokeCallback(const int i, const string &requestStr, const string &replyStr) {
        lock_guard<mutex> lock(cv_m);

        // Copy reply to "replica_reply".
        replica_reply = replyStr;

        // Wake up thread waiting for the reply.
        cv.notify_all();
    }

/* Returns the value corresponding to the supplied key. */
}// namespace hdfsSpec
