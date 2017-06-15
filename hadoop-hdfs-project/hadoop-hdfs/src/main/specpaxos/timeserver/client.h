// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/client.h:
 *   NiStore client-side logic and APIs
 *
 **********************************************************************/


#include "lib/assert.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "common/client.h"
#include "lib/configuration.h"
#include "spec/client.h"
#include "vr/client.h"
#include "fastpaxos/client.h"

#include <iostream>
#include <cstdlib>
#include <string>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <set>
using namespace std;


class Client {
    public:
        Client(string configPath);

        ~Client();

        string Invoke(const string &request);


    private:
        void invokeCallback(const int i, const string &requestStr, const string &replyStr);
        specpaxos::Client *shard;
        UDPTransport transport; // Transport used by paxos client proxies.
        thread *clientTransport; // Thread running the transport event loop.


        mutex cv_m; // Synchronize access to all state in this class and cv.
        condition_variable cv; // To block api calls till a replica reply.

        bool status; // Whether to commit transaction & reply status.
        unsigned int nreplies; // Number of replies received back in 2PC.

        string replica_reply; // Reply back from a shard.

        void run_client(); // Runs the transport event loop.

};
extern "C" void runClient(Client* clientPtr, const char* req, char** reply);
extern "C" void newClientPtr(const char* configDir, Client** ppClient);
 //namespace hdfsSpec


