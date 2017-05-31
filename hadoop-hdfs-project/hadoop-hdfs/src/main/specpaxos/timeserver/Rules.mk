d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), specServer.cc)
SRCS += $(addprefix $(d), client.cc)
$(d)specServer: $(o)specServer.o $(OBJS-fastpaxos-replica) \
  $(OBJS-spec-replica) $(OBJS-vr-replica) $(LIB-udptransport)

BINS += $(d)specServer
