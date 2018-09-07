/* Redis Sentinel implementation
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "hiredis.h"
#include "async.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

extern char **environ;

#define REDIS_SENTINEL_PORT 26379

/* ======================== Sentinel global state =========================== */

/* Address object, used to describe an ip:port pair. */
typedef struct sentinelAddr {
    char *ip;
    int port;
} sentinelAddr;

/* A Sentinel Redis Instance object is monitoring. */
#define SRI_PRIMARY  (1<<0)
#define SRI_REPLICA   (1<<1)
#define SRI_SENTINEL (1<<2)
#define SRI_S_DOWN (1<<3)   /* Subjectively down (no quorum). */
#define SRI_O_DOWN (1<<4)   /* Objectively down (confirmed by others). */
#define SRI_PRIMARY_DOWN (1<<5) /* A Sentinel with this flag set thinks that
                                   its primary is down. */
#define SRI_FAILOVER_IN_PROGRESS (1<<6) /* Failover is in progress for
                                           this primary. */
#define SRI_PROMOTED (1<<7)            /* Replica selected for promotion. */
#define SRI_RECONF_SENT (1<<8)     /* REPLICAOF <newprimary> sent. */
#define SRI_RECONF_INPROG (1<<9)   /* Replica synchronization in progress. */
#define SRI_RECONF_DONE (1<<10)     /* Replica synchronized with new primary. */
#define SRI_FORCE_FAILOVER (1<<11)  /* Force failover with primary up. */
#define SRI_SCRIPT_KILL_SENT (1<<12) /* SCRIPT KILL already sent on -BUSY */

/* Note: times are in milliseconds. */
#define SENTINEL_INFO_PERIOD 10000
#define SENTINEL_PING_PERIOD 1000
#define SENTINEL_ASK_PERIOD 1000
#define SENTINEL_PUBLISH_PERIOD 2000
#define SENTINEL_DEFAULT_DOWN_AFTER 30000
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
#define SENTINEL_TILT_TRIGGER 2000
#define SENTINEL_TILT_PERIOD (SENTINEL_PING_PERIOD*30)
#define SENTINEL_DEFAULT_REPLICA_PRIORITY 100
#define SENTINEL_REPLICA_RECONF_TIMEOUT 10000
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
#define SENTINEL_MIN_LINK_RECONNECT_PERIOD 15000
#define SENTINEL_DEFAULT_FAILOVER_TIMEOUT (60*3*1000)
#define SENTINEL_MAX_PENDING_COMMANDS 100
#define SENTINEL_ELECTION_TIMEOUT 10000
#define SENTINEL_MAX_DESYNC 1000
#define SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG 1

/* Failover machine different states. */
#define SENTINEL_FAILOVER_STATE_NONE 0  /* No failover in progress. */
#define SENTINEL_FAILOVER_STATE_WAIT_START 1  /* Wait for failover_start_time*/
#define SENTINEL_FAILOVER_STATE_SELECT_REPLICA 2 /* Select replica to promote */
#define SENTINEL_FAILOVER_STATE_SEND_REPLICAOF_NOONE 3 /* Replica -> Primary */
#define SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4 /* Wait replica to change role */
#define SENTINEL_FAILOVER_STATE_RECONF_REPLICAS 5 /* REPLICAOF newprimary */
#define SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 6 /* Monitor promoted replica. */

#define SENTINEL_PRIMARY_LINK_STATUS_UP 0
#define SENTINEL_PRIMARY_LINK_STATUS_DOWN 1

/* Generic flags that can be used with different functions.
 * They use higher bits to avoid colliding with the function specific
 * flags. */
#define SENTINEL_NO_FLAGS 0
#define SENTINEL_GENERATE_EVENT (1<<16)
#define SENTINEL_LEADER (1<<17)
#define SENTINEL_OBSERVER (1<<18)

/* Script execution flags and limits. */
#define SENTINEL_SCRIPT_NONE 0
#define SENTINEL_SCRIPT_RUNNING 1
#define SENTINEL_SCRIPT_MAX_QUEUE 256
#define SENTINEL_SCRIPT_MAX_RUNNING 16
#define SENTINEL_SCRIPT_MAX_RUNTIME 60000 /* 60 seconds max exec time. */
#define SENTINEL_SCRIPT_MAX_RETRY 10
#define SENTINEL_SCRIPT_RETRY_DELAY 30000 /* 30 seconds between retries. */

/* SENTINEL SIMULATE-FAILURE command flags. */
#define SENTINEL_SIMFAILURE_NONE 0
#define SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION (1<<0)
#define SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION (1<<1)

/* The link to a sentinelRedisInstance. When we have the same set of Sentinels
 * monitoring many primaries, we have different instances representing the
 * same Sentinels, one per primary, and we need to share the hiredis connections
 * among them. Oherwise if 5 Sentinels are monitoring 100 primaries we create
 * 500 outgoing connections instead of 5.
 *
 * So this structure represents a reference counted link in terms of the two
 * hiredis connections for commands and Pub/Sub, and the fields needed for
 * failure detection, since the ping/pong time are now local to the link: if
 * the link is available, the instance is avaialbe. This way we don't just
 * have 5 connections instead of 500, we also send 5 pings instead of 500.
 *
 * Links are shared only for Sentinels: primary and replica instances have
 * a link with refcount = 1, always. */
typedef struct instanceLink {
    int refcount;          /* Number of sentinelRedisInstance owners. */
    int disconnected;      /* Non-zero if we need to reconnect cc or pc. */
    int pending_commands;  /* Number of commands sent waiting for a reply. */
    redisAsyncContext *cc; /* Hiredis context for commands. */
    redisAsyncContext *pc; /* Hiredis context for Pub / Sub. */
    mstime_t cc_conn_time; /* cc connection time. */
    mstime_t pc_conn_time; /* pc connection time. */
    mstime_t pc_last_activity; /* Last time we received any message. */
    mstime_t last_avail_time; /* Last time the instance replied to ping with
                                 a reply we consider valid. */
    mstime_t act_ping_time;   /* Time at which the last pending ping (no pong
                                 received after it) was sent. This field is
                                 set to 0 when a pong is received, and set again
                                 to the current time if the value is 0 and a new
                                 ping is sent. */
    mstime_t last_ping_time;  /* Time at which we sent the last ping. This is
                                 only used to avoid sending too many pings
                                 during failure. Idle time is computed using
                                 the act_ping_time field. */
    mstime_t last_pong_time;  /* Last time the instance replied to ping,
                                 whatever the reply was. That's used to check
                                 if the link is idle and must be reconnected. */
    mstime_t last_reconn_time;  /* Last reconnection attempt performed when
                                   the link was down. */
} instanceLink;

typedef struct sentinelRedisInstance {
    int flags;      /* See SRI_... defines */
    char *name;     /* Primary name from the point of view of this sentinel. */
    char *runid;    /* Run ID of this instance, or unique ID if is a Sentinel.*/
    uint64_t config_epoch;  /* Configuration epoch. */
    sentinelAddr *addr; /* Primary host. */
    instanceLink *link; /* Link to the instance, may be shared for Sentinels. */
    mstime_t last_pub_time;   /* Last time we sent hello via Pub/Sub. */
    mstime_t last_hello_time; /* Only used if SRI_SENTINEL is set. Last time
                                 we received a hello from this Sentinel
                                 via Pub/Sub. */
    mstime_t last_primary_down_reply_time; /* Time of last reply to
                                             SENTINEL is-primary-down command. */
    mstime_t s_down_since_time; /* Subjectively down since time. */
    mstime_t o_down_since_time; /* Objectively down since time. */
    mstime_t down_after_period; /* Consider it down after that period. */
    mstime_t info_refresh;  /* Time at which we received INFO output from it. */
    dict *renamed_commands;     /* Commands renamed in this instance:
                                   Sentinel will use the alternative commands
                                   mapped on this table to send things like
                                   REPLICAOF, CONFING, INFO, ... */

    /* Role and the first time we observed it.
     * This is useful in order to delay replacing what the instance reports
     * with our own configuration. We need to always wait some time in order
     * to give a chance to the leader to report the new configuration before
     * we do silly things. */
    int role_reported;
    mstime_t role_reported_time;
    mstime_t replica_conf_change_time; /* Last time replica primary addr changed. */

    /* Primary specific. */
    dict *sentinels;    /* Other sentinels monitoring the same primary. */
    dict *replicas;       /* Replicas for this primary instance. */
    unsigned int quorum;/* Number of sentinels that need to agree on failure. */
    int parallel_syncs; /* How many replicas to reconfigure at same time. */
    char *auth_pass;    /* Password to use for AUTH against primary & replicas. */

    /* Replica specific. */
    mstime_t primary_link_down_time; /* Replica replication link down time. */
    int replica_priority; /* Replica priority according to its INFO output. */
    mstime_t replica_reconf_sent_time; /* Time at which we sent REPLICA OF <new> */
    struct sentinelRedisInstance *primary; /* Primary instance if it's replica. */
    char *replica_primary_host;    /* Primary host as reported by INFO */
    int replica_primary_port;      /* Primary port as reported by INFO */
    int replica_primary_link_status; /* Primary link status as reported by INFO */
    unsigned long long replica_repl_offset; /* Replica replication offset. */
    /* Failover */
    char *leader;       /* If this is a primary instance, this is the runid of
                           the Sentinel that should perform the failover. If
                           this is a Sentinel, this is the runid of the Sentinel
                           that this Sentinel voted as leader. */
    uint64_t leader_epoch; /* Epoch of the 'leader' field. */
    uint64_t failover_epoch; /* Epoch of the currently started failover. */
    int failover_state; /* See SENTINEL_FAILOVER_STATE_* defines. */
    mstime_t failover_state_change_time;
    mstime_t failover_start_time;   /* Last failover attempt start time. */
    mstime_t failover_timeout;      /* Max time to refresh failover state. */
    mstime_t failover_delay_logged; /* For what failover_start_time value we
                                       logged the failover delay. */
    struct sentinelRedisInstance *promoted_replica; /* Promoted replica instance. */
    /* Scripts executed to notify admin or reconfigure clients: when they
     * are set to NULL no script is executed. */
    char *notification_script;
    char *client_reconfig_script;
    sds info; /* cached INFO output */
} sentinelRedisInstance;

/* Main state. */
struct sentinelState {
    char myid[CONFIG_RUN_ID_SIZE+1]; /* This sentinel ID. */
    uint64_t current_epoch;         /* Current epoch. */
    dict *primaries;      /* Dictionary of primary sentinelRedisInstances.
                           Key is the instance name, value is the
                           sentinelRedisInstance structure pointer. */
    int tilt;           /* Are we in TILT mode? */
    int running_scripts;    /* Number of scripts in execution right now. */
    mstime_t tilt_start_time;       /* When TITL started. */
    mstime_t previous_time;         /* Last time we ran the time handler. */
    list *scripts_queue;            /* Queue of user scripts to execute. */
    char *announce_ip;  /* IP addr that is gossiped to other sentinels if
                           not NULL. */
    int announce_port;  /* Port that is gossiped to other sentinels if
                           non zero. */
    unsigned long simfailure_flags; /* Failures simulation. */
    int deny_scripts_reconfig; /* Allow SENTINEL SET ... to change script
                                  paths at runtime? */
} sentinel;

/* A script execution job. */
typedef struct sentinelScriptJob {
    int flags;              /* Script job flags: SENTINEL_SCRIPT_* */
    int retry_num;          /* Number of times we tried to execute it. */
    char **argv;            /* Arguments to call the script. */
    mstime_t start_time;    /* Script execution time if the script is running,
                               otherwise 0 if we are allowed to retry the
                               execution at any time. If the script is not
                               running and it's not 0, it means: do not run
                               before the specified time. */
    pid_t pid;              /* Script execution pid. */
} sentinelScriptJob;

/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */

typedef struct redisAeEvents {
    redisAsyncContext *context;
    aeEventLoop *loop;
    int fd;
    int reading, writing;
} redisAeEvents;

static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleRead(e->context);
}

static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleWrite(e->context);
}

static void redisAeAddRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(loop,e->fd,AE_READABLE,redisAeReadEvent,e);
    }
}

static void redisAeDelRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(loop,e->fd,AE_READABLE);
    }
}

static void redisAeAddWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(loop,e->fd,AE_WRITABLE,redisAeWriteEvent,e);
    }
}

static void redisAeDelWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(loop,e->fd,AE_WRITABLE);
    }
}

static void redisAeCleanup(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return C_ERR;

    /* Create container for context and r/w events */
    e = (redisAeEvents*)zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return C_OK;
}

/* ============================= Prototypes ================================= */

void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status);
void sentinelDisconnectCallback(const redisAsyncContext *c, int status);
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata);
sentinelRedisInstance *sentinelGetPrimaryByName(char *name);
char *sentinelGetSubjectiveLeader(sentinelRedisInstance *primary);
char *sentinelGetObjectiveLeader(sentinelRedisInstance *primary);
int yesnotoi(char *s);
void instanceLinkConnectionError(const redisAsyncContext *c);
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri);
void sentinelAbortFailover(sentinelRedisInstance *ri);
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri, const char *fmt, ...);
sentinelRedisInstance *sentinelSelectReplica(sentinelRedisInstance *primary);
void sentinelScheduleScriptExecution(char *path, ...);
void sentinelStartFailover(sentinelRedisInstance *primary);
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata);
int sentinelSendReplicaOf(sentinelRedisInstance *ri, char *host, int port);
char *sentinelVoteLeader(sentinelRedisInstance *primary, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch);
void sentinelFlushConfig(void);
void sentinelGenerateInitialMonitorEvents(void);
int sentinelSendPing(sentinelRedisInstance *ri);
int sentinelForceHelloUpdateForPrimary(sentinelRedisInstance *primary);
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid);
void sentinelSimFailureCrash(void);

/* ========================= Dictionary types =============================== */

uint64_t dictSdsHash(const void *key);
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void releaseSentinelRedisInstance(sentinelRedisInstance *ri);

void dictInstancesValDestructor (void *privdata, void *obj) {
    UNUSED(privdata);
    releaseSentinelRedisInstance(obj);
}

/* Instance name (sds) -> instance (sentinelRedisInstance pointer)
 *
 * also used for: sentinelRedisInstance->sentinels dictionary that maps
 * sentinels ip:port to last seen time in Pub/Sub hello message. */
dictType instancesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    dictInstancesValDestructor /* val destructor */
};

/* Instance runid (sds) -> votes (long casted to void*)
 *
 * This is useful into sentinelGetObjectiveLeader() function in order to
 * count the votes and understand who is the leader. */
dictType leaderVotesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL                       /* val destructor */
};

/* Instance renamed commands table. */
dictType renamedCommandsDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    dictSdsDestructor          /* val destructor */
};

/* =========================== Initialization =============================== */

void sentinelCommand(client *c);
void sentinelInfoCommand(client *c);
void sentinelSetCommand(client *c);
void sentinelPublishCommand(client *c);
void sentinelRoleCommand(client *c);

struct redisCommand sentinelcmds[] = {
    {"ping",pingCommand,1,"",0,NULL,0,0,0,0,0},
    {"sentinel",sentinelCommand,-2,"",0,NULL,0,0,0,0,0},
    {"subscribe",subscribeCommand,-2,"",0,NULL,0,0,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,"",0,NULL,0,0,0,0,0},
    {"psubscribe",psubscribeCommand,-2,"",0,NULL,0,0,0,0,0},
    {"punsubscribe",punsubscribeCommand,-1,"",0,NULL,0,0,0,0,0},
    {"publish",sentinelPublishCommand,3,"",0,NULL,0,0,0,0,0},
    {"info",sentinelInfoCommand,-1,"",0,NULL,0,0,0,0,0},
    {"role",sentinelRoleCommand,1,"l",0,NULL,0,0,0,0,0},
    {"client",clientCommand,-2,"rs",0,NULL,0,0,0,0,0},
    {"shutdown",shutdownCommand,-1,"",0,NULL,0,0,0,0,0}
};

/* This function overwrites a few normal Redis config default with Sentinel
 * specific defaults. */
void initSentinelConfig(void) {
    server.port = REDIS_SENTINEL_PORT;
}

/* Perform the Sentinel mode initialization. */
void initSentinel(void) {
    unsigned int j;

    /* Remove usual Redis commands from the command table, then just add
     * the SENTINEL command. */
    dictEmpty(server.commands,NULL);
    for (j = 0; j < sizeof(sentinelcmds)/sizeof(sentinelcmds[0]); j++) {
        int retval;
        struct redisCommand *cmd = sentinelcmds+j;

        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);
    }

    /* Initialize various data structures. */
    sentinel.current_epoch = 0;
    sentinel.primaries = dictCreate(&instancesDictType,NULL);
    sentinel.tilt = 0;
    sentinel.tilt_start_time = 0;
    sentinel.previous_time = mstime();
    sentinel.running_scripts = 0;
    sentinel.scripts_queue = listCreate();
    sentinel.announce_ip = NULL;
    sentinel.announce_port = 0;
    sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
    sentinel.deny_scripts_reconfig = SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG;
    memset(sentinel.myid,0,sizeof(sentinel.myid));
}

/* This function gets called when the server is in Sentinel mode, started,
 * loaded the configuration, and is ready for normal operations. */
void sentinelIsRunning(void) {
    int j;

    if (server.configfile == NULL) {
        serverLog(LL_WARNING,
            "Sentinel started without a config file. Exiting...");
        exit(1);
    } else if (access(server.configfile,W_OK) == -1) {
        serverLog(LL_WARNING,
            "Sentinel config file %s is not writable: %s. Exiting...",
            server.configfile,strerror(errno));
        exit(1);
    }

    /* If this Sentinel has yet no ID set in the configuration file, we
     * pick a random one and persist the config on disk. From now on this
     * will be this Sentinel ID across restarts. */
    for (j = 0; j < CONFIG_RUN_ID_SIZE; j++)
        if (sentinel.myid[j] != 0) break;

    if (j == CONFIG_RUN_ID_SIZE) {
        /* Pick ID and persist the config. */
        getRandomHexChars(sentinel.myid,CONFIG_RUN_ID_SIZE);
        sentinelFlushConfig();
    }

    /* Log its ID to make debugging of issues simpler. */
    serverLog(LL_WARNING,"Sentinel ID is %s", sentinel.myid);

    /* We want to generate a +monitor event for every configured primary
     * at startup. */
    sentinelGenerateInitialMonitorEvents();
}

/* ============================== sentinelAddr ============================== */

/* Create a sentinelAddr object and return it on success.
 * On error NULL is returned and errno is set to:
 *  ENOENT: Can't resolve the hostname.
 *  EINVAL: Invalid port number.
 */
sentinelAddr *createSentinelAddr(char *hostname, int port) {
    char ip[NET_IP_STR_LEN];
    sentinelAddr *sa;

    if (port < 0 || port > 65535) {
        errno = EINVAL;
        return NULL;
    }
    if (anetResolve(NULL,hostname,ip,sizeof(ip)) == ANET_ERR) {
        errno = ENOENT;
        return NULL;
    }
    sa = zmalloc(sizeof(*sa));
    sa->ip = sdsnew(ip);
    sa->port = port;
    return sa;
}

/* Return a duplicate of the source address. */
sentinelAddr *dupSentinelAddr(sentinelAddr *src) {
    sentinelAddr *sa;

    sa = zmalloc(sizeof(*sa));
    sa->ip = sdsnew(src->ip);
    sa->port = src->port;
    return sa;
}

/* Free a Sentinel address. Can't fail. */
void releaseSentinelAddr(sentinelAddr *sa) {
    sdsfree(sa->ip);
    zfree(sa);
}

/* Return non-zero if two addresses are equal. */
int sentinelAddrIsEqual(sentinelAddr *a, sentinelAddr *b) {
    return a->port == b->port && !strcasecmp(a->ip,b->ip);
}

/* =========================== Events notification ========================== */

/* Send an event to log, pub/sub, user notification script.
 *
 * 'level' is the log level for logging. Only LL_WARNING events will trigger
 * the execution of the user notification script.
 *
 * 'type' is the message type, also used as a pub/sub channel name.
 *
 * 'ri', is the redis instance target of this event if applicable, and is
 * used to obtain the path of the notification script to execute.
 *
 * The remaining arguments are printf-alike.
 * If the format specifier starts with the two characters "%@" then ri is
 * not NULL, and the message is prefixed with an instance identifier in the
 * following format:
 *
 *  <instance type> <instance name> <ip> <port>
 *
 *  If the instance type is not primary, than the additional string is
 *  added to specify the originating primary:
 *
 *  @ <primary name> <primary ip> <primary port>
 *
 *  Any other specifier after "%@" is processed by printf itself.
 */
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri,
                   const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];
    robj *channel, *payload;

    /* Handle %@ */
    if (fmt[0] == '%' && fmt[1] == '@') {
        sentinelRedisInstance *primary = (ri->flags & SRI_PRIMARY) ?
                                         NULL : ri->primary;

        if (primary) {
            snprintf(msg, sizeof(msg), "%s %s %s %d @ %s %s %d",
                sentinelRedisInstanceTypeStr(ri),
                ri->name, ri->addr->ip, ri->addr->port,
                primary->name, primary->addr->ip, primary->addr->port);
        } else {
            snprintf(msg, sizeof(msg), "%s %s %s %d",
                sentinelRedisInstanceTypeStr(ri),
                ri->name, ri->addr->ip, ri->addr->port);
        }
        fmt += 2;
    } else {
        msg[0] = '\0';
    }

    /* Use vsprintf for the rest of the formatting if any. */
    if (fmt[0] != '\0') {
        va_start(ap, fmt);
        vsnprintf(msg+strlen(msg), sizeof(msg)-strlen(msg), fmt, ap);
        va_end(ap);
    }

    /* Log the message if the log level allows it to be logged. */
    if (level >= server.verbosity)
        serverLog(level,"%s %s",type,msg);

    /* Publish the message via Pub/Sub if it's not a debugging one. */
    if (level != LL_DEBUG) {
        channel = createStringObject(type,strlen(type));
        payload = createStringObject(msg,strlen(msg));
        pubsubPublishMessage(channel,payload);
        decrRefCount(channel);
        decrRefCount(payload);
    }

    /* Call the notification script if applicable. */
    if (level == LL_WARNING && ri != NULL) {
        sentinelRedisInstance *primary = (ri->flags & SRI_PRIMARY) ?
                                         ri : ri->primary;
        if (primary && primary->notification_script) {
            sentinelScheduleScriptExecution(primary->notification_script,
                type,msg,NULL);
        }
    }
}

/* This function is called only at startup and is used to generate a
 * +monitor event for every configured primary. The same events are also
 * generated when a primary to monitor is added at runtime via the
 * SENTINEL MONITOR command. */
void sentinelGenerateInitialMonitorEvents(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(sentinel.primaries);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        sentinelEvent(LL_WARNING,"+monitor",ri,"%@ quorum %d",ri->quorum);
    }
    dictReleaseIterator(di);
}

/* ============================ script execution ============================ */

/* Release a script job structure and all the associated data. */
void sentinelReleaseScriptJob(sentinelScriptJob *sj) {
    int j = 0;

    while(sj->argv[j]) sdsfree(sj->argv[j++]);
    zfree(sj->argv);
    zfree(sj);
}

#define SENTINEL_SCRIPT_MAX_ARGS 16
void sentinelScheduleScriptExecution(char *path, ...) {
    va_list ap;
    char *argv[SENTINEL_SCRIPT_MAX_ARGS+1];
    int argc = 1;
    sentinelScriptJob *sj;

    va_start(ap, path);
    while(argc < SENTINEL_SCRIPT_MAX_ARGS) {
        argv[argc] = va_arg(ap,char*);
        if (!argv[argc]) break;
        argv[argc] = sdsnew(argv[argc]); /* Copy the string. */
        argc++;
    }
    va_end(ap);
    argv[0] = sdsnew(path);

    sj = zmalloc(sizeof(*sj));
    sj->flags = SENTINEL_SCRIPT_NONE;
    sj->retry_num = 0;
    sj->argv = zmalloc(sizeof(char*)*(argc+1));
    sj->start_time = 0;
    sj->pid = 0;
    memcpy(sj->argv,argv,sizeof(char*)*(argc+1));

    listAddNodeTail(sentinel.scripts_queue,sj);

    /* Remove the oldest non running script if we already hit the limit. */
    if (listLength(sentinel.scripts_queue) > SENTINEL_SCRIPT_MAX_QUEUE) {
        listNode *ln;
        listIter li;

        listRewind(sentinel.scripts_queue,&li);
        while ((ln = listNext(&li)) != NULL) {
            sj = ln->value;

            if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;
            /* The first node is the oldest as we add on tail. */
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            break;
        }
        serverAssert(listLength(sentinel.scripts_queue) <=
                    SENTINEL_SCRIPT_MAX_QUEUE);
    }
}

/* Lookup a script in the scripts queue via pid, and returns the list node
 * (so that we can easily remove it from the queue if needed). */
listNode *sentinelGetScriptListNodeByPid(pid_t pid) {
    listNode *ln;
    listIter li;

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if ((sj->flags & SENTINEL_SCRIPT_RUNNING) && sj->pid == pid)
            return ln;
    }
    return NULL;
}

/* Run pending scripts if we are not already at max number of running
 * scripts. */
void sentinelRunPendingScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    /* Find jobs that are not running and run them, from the top to the
     * tail of the queue, so we run older jobs first. */
    listRewind(sentinel.scripts_queue,&li);
    while (sentinel.running_scripts < SENTINEL_SCRIPT_MAX_RUNNING &&
           (ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;
        pid_t pid;

        /* Skip if already running. */
        if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;

        /* Skip if it's a retry, but not enough time has elapsed. */
        if (sj->start_time && sj->start_time > now) continue;

        sj->flags |= SENTINEL_SCRIPT_RUNNING;
        sj->start_time = mstime();
        sj->retry_num++;
        pid = fork();

        if (pid == -1) {
            /* Parent (fork error).
             * We report fork errors as signal 99, in order to unify the
             * reporting with other kind of errors. */
            sentinelEvent(LL_WARNING,"-script-error",NULL,
                          "%s %d %d", sj->argv[0], 99, 0);
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
        } else if (pid == 0) {
            /* Child */
            execve(sj->argv[0],sj->argv,environ);
            /* If we are here an error occurred. */
            _exit(2); /* Don't retry execution. */
        } else {
            sentinel.running_scripts++;
            sj->pid = pid;
            sentinelEvent(LL_DEBUG,"+script-child",NULL,"%ld",(long)pid);
        }
    }
}

/* How much to delay the execution of a script that we need to retry after
 * an error?
 *
 * We double the retry delay for every further retry we do. So for instance
 * if RETRY_DELAY is set to 30 seconds and the max number of retries is 10
 * starting from the second attempt to execute the script the delays are:
 * 30 sec, 60 sec, 2 min, 4 min, 8 min, 16 min, 32 min, 64 min, 128 min. */
mstime_t sentinelScriptRetryDelay(int retry_num) {
    mstime_t delay = SENTINEL_SCRIPT_RETRY_DELAY;

    while (retry_num-- > 1) delay *= 2;
    return delay;
}

/* Check for scripts that terminated, and remove them from the queue if the
 * script terminated successfully. If instead the script was terminated by
 * a signal, or returned exit code "1", it is scheduled to run again if
 * the max number of retries did not already elapsed. */
void sentinelCollectTerminatedScripts(void) {
    int statloc;
    pid_t pid;

    while ((pid = wait3(&statloc,WNOHANG,NULL)) > 0) {
        int exitcode = WEXITSTATUS(statloc);
        int bysignal = 0;
        listNode *ln;
        sentinelScriptJob *sj;

        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);
        sentinelEvent(LL_DEBUG,"-script-child",NULL,"%ld %d %d",
            (long)pid, exitcode, bysignal);

        ln = sentinelGetScriptListNodeByPid(pid);
        if (ln == NULL) {
            serverLog(LL_WARNING,"wait3() returned a pid (%ld) we can't find in our scripts execution queue!", (long)pid);
            continue;
        }
        sj = ln->value;

        /* If the script was terminated by a signal or returns an
         * exit code of "1" (that means: please retry), we reschedule it
         * if the max number of retries is not already reached. */
        if ((bysignal || exitcode == 1) &&
            sj->retry_num != SENTINEL_SCRIPT_MAX_RETRY)
        {
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
            sj->start_time = mstime() +
                             sentinelScriptRetryDelay(sj->retry_num);
        } else {
            /* Otherwise let's remove the script, but log the event if the
             * execution did not terminated in the best of the ways. */
            if (bysignal || exitcode != 0) {
                sentinelEvent(LL_WARNING,"-script-error",NULL,
                              "%s %d %d", sj->argv[0], bysignal, exitcode);
            }
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            sentinel.running_scripts--;
        }
    }
}

/* Kill scripts in timeout, they'll be collected by the
 * sentinelCollectTerminatedScripts() function. */
void sentinelKillTimedoutScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if (sj->flags & SENTINEL_SCRIPT_RUNNING &&
            (now - sj->start_time) > SENTINEL_SCRIPT_MAX_RUNTIME)
        {
            sentinelEvent(LL_WARNING,"-script-timeout",NULL,"%s %ld",
                sj->argv[0], (long)sj->pid);
            kill(sj->pid,SIGKILL);
        }
    }
}

/* Implements SENTINEL PENDING-SCRIPTS command. */
void sentinelPendingScriptsCommand(client *c) {
    listNode *ln;
    listIter li;

    addReplyMultiBulkLen(c,listLength(sentinel.scripts_queue));
    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;
        int j = 0;

        addReplyMultiBulkLen(c,10);

        addReplyBulkCString(c,"argv");
        while (sj->argv[j]) j++;
        addReplyMultiBulkLen(c,j);
        j = 0;
        while (sj->argv[j]) addReplyBulkCString(c,sj->argv[j++]);

        addReplyBulkCString(c,"flags");
        addReplyBulkCString(c,
            (sj->flags & SENTINEL_SCRIPT_RUNNING) ? "running" : "scheduled");

        addReplyBulkCString(c,"pid");
        addReplyBulkLongLong(c,sj->pid);

        if (sj->flags & SENTINEL_SCRIPT_RUNNING) {
            addReplyBulkCString(c,"run-time");
            addReplyBulkLongLong(c,mstime() - sj->start_time);
        } else {
            mstime_t delay = sj->start_time ? (sj->start_time-mstime()) : 0;
            if (delay < 0) delay = 0;
            addReplyBulkCString(c,"run-delay");
            addReplyBulkLongLong(c,delay);
        }

        addReplyBulkCString(c,"retry-num");
        addReplyBulkLongLong(c,sj->retry_num);
    }
}

/* This function calls, if any, the client reconfiguration script with the
 * following parameters:
 *
 * <primary-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * It is called every time a failover is performed.
 *
 * <state> is currently always "failover".
 * <role> is either "leader" or "observer".
 *
 * from/to fields are respectively primary -> promoted replica addresses for
 * "start" and "end". */
void sentinelCallClientReconfScript(sentinelRedisInstance *primary, int role, char *state, sentinelAddr *from, sentinelAddr *to) {
    char fromport[32], toport[32];

    if (primary->client_reconfig_script == NULL) return;
    ll2string(fromport,sizeof(fromport),from->port);
    ll2string(toport,sizeof(toport),to->port);
    sentinelScheduleScriptExecution(primary->client_reconfig_script,
        primary->name,
        (role == SENTINEL_LEADER) ? "leader" : "observer",
        state, from->ip, fromport, to->ip, toport, NULL);
}

/* =============================== instanceLink ============================= */

/* Create a not yet connected link object. */
instanceLink *createInstanceLink(void) {
    instanceLink *link = zmalloc(sizeof(*link));

    link->refcount = 1;
    link->disconnected = 1;
    link->pending_commands = 0;
    link->cc = NULL;
    link->pc = NULL;
    link->cc_conn_time = 0;
    link->pc_conn_time = 0;
    link->last_reconn_time = 0;
    link->pc_last_activity = 0;
    /* We set the act_ping_time to "now" even if we actually don't have yet
     * a connection with the node, nor we sent a ping.
     * This is useful to detect a timeout in case we'll not be able to connect
     * with the node at all. */
    link->act_ping_time = mstime();
    link->last_ping_time = 0;
    link->last_avail_time = mstime();
    link->last_pong_time = mstime();
    return link;
}

/* Disconnect an hiredis connection in the context of an instance link. */
void instanceLinkCloseConnection(instanceLink *link, redisAsyncContext *c) {
    if (c == NULL) return;

    if (link->cc == c) {
        link->cc = NULL;
        link->pending_commands = 0;
    }
    if (link->pc == c) link->pc = NULL;
    c->data = NULL;
    link->disconnected = 1;
    redisAsyncFree(c);
}

/* Decrement the refcount of a link object, if it drops to zero, actually
 * free it and return NULL. Otherwise don't do anything and return the pointer
 * to the object.
 *
 * If we are not going to free the link and ri is not NULL, we rebind all the
 * pending requests in link->cc (hiredis connection for commands) to a
 * callback that will just ignore them. This is useful to avoid processing
 * replies for an instance that no longer exists. */
instanceLink *releaseInstanceLink(instanceLink *link, sentinelRedisInstance *ri)
{
    serverAssert(link->refcount > 0);
    link->refcount--;
    if (link->refcount != 0) {
        if (ri && ri->link->cc) {
            /* This instance may have pending callbacks in the hiredis async
             * context, having as 'privdata' the instance that we are going to
             * free. Let's rewrite the callback list, directly exploiting
             * hiredis internal data structures, in order to bind them with
             * a callback that will ignore the reply at all. */
            redisCallback *cb;
            redisCallbackList *callbacks = &link->cc->replies;

            cb = callbacks->head;
            while(cb) {
                if (cb->privdata == ri) {
                    cb->fn = sentinelDiscardReplyCallback;
                    cb->privdata = NULL; /* Not strictly needed. */
                }
                cb = cb->next;
            }
        }
        return link; /* Other active users. */
    }

    instanceLinkCloseConnection(link,link->cc);
    instanceLinkCloseConnection(link,link->pc);
    zfree(link);
    return NULL;
}

/* This function will attempt to share the instance link we already have
 * for the same Sentinel in the context of a different primary, with the
 * instance we are passing as argument.
 *
 * This way multiple Sentinel objects that refer all to the same physical
 * Sentinel instance but in the context of different primaries will use
 * a single connection, will send a single PING per second for failure
 * detection and so forth.
 *
 * Return C_OK if a matching Sentinel was found in the context of a
 * different primary and sharing was performed. Otherwise C_ERR
 * is returned. */
int sentinelTryConnectionSharing(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;

    if (ri->runid == NULL) return C_ERR; /* No way to identify it. */
    if (ri->link->refcount > 1) return C_ERR; /* Already shared. */

    di = dictGetIterator(sentinel.primaries);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *primary = dictGetVal(de), *match;
        /* We want to share with the same physical Sentinel referenced
         * in other primaries, so skip our primary. */
        if (primary == ri->primary) continue;
        match = getSentinelRedisInstanceByAddrAndRunID(primary->sentinels,
                                                       NULL,0,ri->runid);
        if (match == NULL) continue; /* No match. */
        if (match == ri) continue; /* Should never happen but... safer. */

        /* We identified a matching Sentinel, great! Let's free our link
         * and use the one of the matching Sentinel. */
        releaseInstanceLink(ri->link,NULL);
        ri->link = match->link;
        match->link->refcount++;
        return C_OK;
    }
    dictReleaseIterator(di);
    return C_ERR;
}

/* When we detect a Sentinel to switch address (reporting a different IP/port
 * pair in Hello messages), let's update all the matching Sentinels in the
 * context of other primaries as well and disconnect the links, so that everybody
 * will be updated.
 *
 * Return the number of updated Sentinel addresses. */
int sentinelUpdateSentinelAddressInAllPrimaries(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;
    int reconfigured = 0;

    di = dictGetIterator(sentinel.primaries);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *primary = dictGetVal(de), *match;
        match = getSentinelRedisInstanceByAddrAndRunID(primary->sentinels,
                                                       NULL,0,ri->runid);
        /* If there is no match, this primary does not know about this
         * Sentinel, try with the next one. */
        if (match == NULL) continue;

        /* Disconnect the old links if connected. */
        if (match->link->cc != NULL)
            instanceLinkCloseConnection(match->link,match->link->cc);
        if (match->link->pc != NULL)
            instanceLinkCloseConnection(match->link,match->link->pc);

        if (match == ri) continue; /* Address already updated for it. */

        /* Update the address of the matching Sentinel by copying the address
         * of the Sentinel object that received the address update. */
        releaseSentinelAddr(match->addr);
        match->addr = dupSentinelAddr(ri->addr);
        reconfigured++;
    }
    dictReleaseIterator(di);
    if (reconfigured)
        sentinelEvent(LL_NOTICE,"+sentinel-address-update", ri,
                    "%@ %d additional matching instances", reconfigured);
    return reconfigured;
}

/* This function is called when an hiredis connection reported an error.
 * We set it to NULL and mark the link as disconnected so that it will be
 * reconnected again.
 *
 * Note: we don't free the hiredis context as hiredis will do it for us
 * for async connections. */
void instanceLinkConnectionError(const redisAsyncContext *c) {
    instanceLink *link = c->data;
    int pubsub;

    if (!link) return;

    pubsub = (link->pc == c);
    if (pubsub)
        link->pc = NULL;
    else
        link->cc = NULL;
    link->disconnected = 1;
}

/* Hiredis connection established / disconnected callbacks. We need them
 * just to cleanup our link state. */
void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status) {
    if (status != C_OK) instanceLinkConnectionError(c);
}

void sentinelDisconnectCallback(const redisAsyncContext *c, int status) {
    UNUSED(status);
    instanceLinkConnectionError(c);
}

/* ========================== sentinelRedisInstance ========================= */

/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 * runid: set to NULL but will be populated once INFO output is received.
 * info_refresh: is set to 0 to mean that we never received INFO so far.
 *
 * If SRI_PRIMARY is set into initial flags the instance is added to
 * sentinel.primaries table.
 *
 * if SRI_REPLICA or SRI_SENTINEL is set then 'primary' must be not NULL and the
 * instance is added into primary->replicas or primary->sentinels table.
 *
 * If the instance is a replica or sentinel, the name parameter is ignored and
 * is created automatically as hostname:port.
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * The function may also fail and return NULL with errno set to EBUSY if
 * a primary with the same name, a replica with the same address, or a sentinel
 * with the same ID already exists. */

sentinelRedisInstance *createSentinelRedisInstance(char *name, int flags, char *hostname, int port, int quorum, sentinelRedisInstance *primary) {
    sentinelRedisInstance *ri;
    sentinelAddr *addr;
    dict *table = NULL;
    char replicaname[NET_PEER_ID_LEN], *sdsname;

    serverAssert(flags & (SRI_PRIMARY|SRI_REPLICA|SRI_SENTINEL));
    serverAssert((flags & SRI_PRIMARY) || primary != NULL);

    /* Check address validity. */
    addr = createSentinelAddr(hostname,port);
    if (addr == NULL) return NULL;

    /* For replicas use ip:port as name. */
    if (flags & SRI_REPLICA) {
        anetFormatAddr(replicaname, sizeof(replicaname), hostname, port);
        name = replicaname;
    }

    /* Make sure the entry is not duplicated. This may happen when the same
     * name for a primary is used multiple times inside the configuration or
     * if we try to add multiple times a replica or sentinel with same ip/port
     * to a primary. */
    if (flags & SRI_PRIMARY) table = sentinel.primaries;
    else if (flags & SRI_REPLICA) table = primary->replicas;
    else if (flags & SRI_SENTINEL) table = primary->sentinels;
    sdsname = sdsnew(name);
    if (dictFind(table,sdsname)) {
        releaseSentinelAddr(addr);
        sdsfree(sdsname);
        errno = EBUSY;
        return NULL;
    }

    /* Create the instance object. */
    ri = zmalloc(sizeof(*ri));
    /* Note that all the instances are started in the disconnected state,
     * the event loop will take care of connecting them. */
    ri->flags = flags;
    ri->name = sdsname;
    ri->runid = NULL;
    ri->config_epoch = 0;
    ri->addr = addr;
    ri->link = createInstanceLink();
    ri->last_pub_time = mstime();
    ri->last_hello_time = mstime();
    ri->last_primary_down_reply_time = mstime();
    ri->s_down_since_time = 0;
    ri->o_down_since_time = 0;
    ri->down_after_period = primary ? primary->down_after_period :
                            SENTINEL_DEFAULT_DOWN_AFTER;
    ri->primary_link_down_time = 0;
    ri->auth_pass = NULL;
    ri->replica_priority = SENTINEL_DEFAULT_REPLICA_PRIORITY;
    ri->replica_reconf_sent_time = 0;
    ri->replica_primary_host = NULL;
    ri->replica_primary_port = 0;
    ri->replica_primary_link_status = SENTINEL_PRIMARY_LINK_STATUS_DOWN;
    ri->replica_repl_offset = 0;
    ri->sentinels = dictCreate(&instancesDictType,NULL);
    ri->quorum = quorum;
    ri->parallel_syncs = SENTINEL_DEFAULT_PARALLEL_SYNCS;
    ri->primary = primary;
    ri->replicas = dictCreate(&instancesDictType,NULL);
    ri->info_refresh = 0;
    ri->renamed_commands = dictCreate(&renamedCommandsDictType,NULL);

    /* Failover state. */
    ri->leader = NULL;
    ri->leader_epoch = 0;
    ri->failover_epoch = 0;
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->failover_timeout = SENTINEL_DEFAULT_FAILOVER_TIMEOUT;
    ri->failover_delay_logged = 0;
    ri->promoted_replica = NULL;
    ri->notification_script = NULL;
    ri->client_reconfig_script = NULL;
    ri->info = NULL;

    /* Role */
    ri->role_reported = ri->flags & (SRI_PRIMARY|SRI_REPLICA);
    ri->role_reported_time = mstime();
    ri->replica_conf_change_time = mstime();

    /* Add into the right table. */
    dictAdd(table, ri->name, ri);
    return ri;
}

/* Release this instance and all its replicas, sentinels, hiredis connections.
 * This function does not take care of unlinking the instance from the main
 * primaries table (if it is a primary) or from its primary sentinels/replicas table
 * if it is a replica or sentinel. */
void releaseSentinelRedisInstance(sentinelRedisInstance *ri) {
    /* Release all its replicas or sentinels if any. */
    dictRelease(ri->sentinels);
    dictRelease(ri->replicas);

    /* Disconnect the instance. */
    releaseInstanceLink(ri->link,ri);

    /* Free other resources. */
    sdsfree(ri->name);
    sdsfree(ri->runid);
    sdsfree(ri->notification_script);
    sdsfree(ri->client_reconfig_script);
    sdsfree(ri->replica_primary_host);
    sdsfree(ri->leader);
    sdsfree(ri->auth_pass);
    sdsfree(ri->info);
    releaseSentinelAddr(ri->addr);
    dictRelease(ri->renamed_commands);

    /* Clear state into the primary if needed. */
    if ((ri->flags & SRI_REPLICA) && (ri->flags & SRI_PROMOTED) && ri->primary)
        ri->primary->promoted_replica = NULL;

    zfree(ri);
}

/* Lookup a replica in a primary Redis instance, by ip and port. */
sentinelRedisInstance *sentinelRedisInstanceLookupReplica(
                sentinelRedisInstance *ri, char *ip, int port)
{
    sds key;
    sentinelRedisInstance *replica;
    char buf[NET_PEER_ID_LEN];

    serverAssert(ri->flags & SRI_PRIMARY);
    anetFormatAddr(buf,sizeof(buf),ip,port);
    key = sdsnew(buf);
    replica = dictFetchValue(ri->replicas,key);
    sdsfree(key);
    return replica;
}

/* Return the name of the type of the instance as a string. */
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri) {
    if (ri->flags & SRI_PRIMARY) return "primary";
    else if (ri->flags & SRI_REPLICA) return "replica";
    else if (ri->flags & SRI_SENTINEL) return "sentinel";
    else return "unknown";
}

/* This function remove the Sentinel with the specified ID from the
 * specified primary.
 *
 * If "runid" is NULL the function returns ASAP.
 *
 * This function is useful because on Sentinels address switch, we want to
 * remove our old entry and add a new one for the same ID but with the new
 * address.
 *
 * The function returns 1 if the matching Sentinel was removed, otherwise
 * 0 if there was no Sentinel with this ID. */
int removeMatchingSentinelFromPrimary(sentinelRedisInstance *primary, char *runid) {
    dictIterator *di;
    dictEntry *de;
    int removed = 0;

    if (runid == NULL) return 0;

    di = dictGetSafeIterator(primary->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->runid && strcmp(ri->runid,runid) == 0) {
            dictDelete(primary->sentinels,ri->name);
            removed++;
        }
    }
    dictReleaseIterator(di);
    return removed;
}

/* Search an instance with the same runid, ip and port into a dictionary
 * of instances. Return NULL if not found, otherwise return the instance
 * pointer.
 *
 * runid or ip can be NULL. In such a case the search is performed only
 * by the non-NULL field. */
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *instance = NULL;

    serverAssert(ip || runid);   /* User must pass at least one search param. */
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (runid && !ri->runid) continue;
        if ((runid == NULL || strcmp(ri->runid, runid) == 0) &&
            (ip == NULL || (strcmp(ri->addr->ip, ip) == 0 &&
                            ri->addr->port == port)))
        {
            instance = ri;
            break;
        }
    }
    dictReleaseIterator(di);
    return instance;
}

/* Primary lookup by name */
sentinelRedisInstance *sentinelGetPrimaryByName(char *name) {
    sentinelRedisInstance *ri;
    sds sdsname = sdsnew(name);

    ri = dictFetchValue(sentinel.primaries,sdsname);
    sdsfree(sdsname);
    return ri;
}

/* Add the specified flags to all the instances in the specified dictionary. */
void sentinelAddFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags |= flags;
    }
    dictReleaseIterator(di);
}

/* Remove the specified flags to all the instances in the specified
 * dictionary. */
void sentinelDelFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags &= ~flags;
    }
    dictReleaseIterator(di);
}

/* Reset the state of a monitored primary:
 * 1) Remove all replicas.
 * 2) Remove all sentinels.
 * 3) Remove most of the flags resulting from runtime operations.
 * 4) Reset timers to their default value. For example after a reset it will be
 *    possible to failover again the same primary ASAP, without waiting the
 *    failover timeout delay.
 * 5) In the process of doing this undo the failover if in progress.
 * 6) Disconnect the connections with the primary (will reconnect automatically).
 */

#define SENTINEL_RESET_NO_SENTINELS (1<<0)
void sentinelResetPrimary(sentinelRedisInstance *ri, int flags) {
    serverAssert(ri->flags & SRI_PRIMARY);
    dictRelease(ri->replicas);
    ri->replicas = dictCreate(&instancesDictType,NULL);
    if (!(flags & SENTINEL_RESET_NO_SENTINELS)) {
        dictRelease(ri->sentinels);
        ri->sentinels = dictCreate(&instancesDictType,NULL);
    }
    instanceLinkCloseConnection(ri->link,ri->link->cc);
    instanceLinkCloseConnection(ri->link,ri->link->pc);
    ri->flags &= SRI_PRIMARY;
    if (ri->leader) {
        sdsfree(ri->leader);
        ri->leader = NULL;
    }
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0; /* We can failover again ASAP. */
    ri->promoted_replica = NULL;
    sdsfree(ri->runid);
    sdsfree(ri->replica_primary_host);
    ri->runid = NULL;
    ri->replica_primary_host = NULL;
    ri->link->act_ping_time = mstime();
    ri->link->last_ping_time = 0;
    ri->link->last_avail_time = mstime();
    ri->link->last_pong_time = mstime();
    ri->role_reported_time = mstime();
    ri->role_reported = SRI_PRIMARY;
    if (flags & SENTINEL_GENERATE_EVENT)
        sentinelEvent(LL_WARNING,"+reset-primary",ri,"%@");
}

/* Call sentinelResetPrimary() on every primary with a name matching the specified
 * pattern. */
int sentinelResetPrimariesByPattern(char *pattern, int flags) {
    dictIterator *di;
    dictEntry *de;
    int reset = 0;

    di = dictGetIterator(sentinel.primaries);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->name) {
            if (stringmatch(pattern,ri->name,0)) {
                sentinelResetPrimary(ri,flags);
                reset++;
            }
        }
    }
    dictReleaseIterator(di);
    return reset;
}

/* Reset the specified primary with sentinelResetPrimary(), and also change
 * the ip:port address, but take the name of the instance unmodified.
 *
 * This is used to handle the +switch-primary event.
 *
 * The function returns C_ERR if the address can't be resolved for some
 * reason. Otherwise C_OK is returned.  */
int sentinelResetPrimaryAndChangeAddress(sentinelRedisInstance *primary, char *ip, int port) {
    sentinelAddr *oldaddr, *newaddr;
    sentinelAddr **replicas = NULL;
    int numreplicas = 0, j;
    dictIterator *di;
    dictEntry *de;

    newaddr = createSentinelAddr(ip,port);
    if (newaddr == NULL) return C_ERR;

    /* Make a list of replicas to add back after the reset.
     * Don't include the one having the address we are switching to. */
    di = dictGetIterator(primary->replicas);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *replica = dictGetVal(de);

        if (sentinelAddrIsEqual(replica->addr,newaddr)) continue;
        replicas = zrealloc(replicas,sizeof(sentinelAddr*)*(numreplicas+1));
        replicas[numreplicas++] = createSentinelAddr(replica->addr->ip,
                                                 replica->addr->port);
    }
    dictReleaseIterator(di);

    /* If we are switching to a different address, include the old address
     * as a replica as well, so that we'll be able to sense / reconfigure
     * the old primary. */
    if (!sentinelAddrIsEqual(newaddr,primary->addr)) {
        replicas = zrealloc(replicas,sizeof(sentinelAddr*)*(numreplicas+1));
        replicas[numreplicas++] = createSentinelAddr(primary->addr->ip,
                                                 primary->addr->port);
    }

    /* Reset and switch address. */
    sentinelResetPrimary(primary,SENTINEL_RESET_NO_SENTINELS);
    oldaddr = primary->addr;
    primary->addr = newaddr;
    primary->o_down_since_time = 0;
    primary->s_down_since_time = 0;

    /* Add replicas back. */
    for (j = 0; j < numreplicas; j++) {
        sentinelRedisInstance *replica;

        replica = createSentinelRedisInstance(NULL,SRI_REPLICA,replicas[j]->ip,
                    replicas[j]->port, primary->quorum, primary);
        releaseSentinelAddr(replicas[j]);
        if (replica) sentinelEvent(LL_NOTICE,"+replica",replica,"%@");
    }
    zfree(replicas);

    /* Release the old address at the end so we are safe even if the function
     * gets the primary->addr->ip and primary->addr->port as arguments. */
    releaseSentinelAddr(oldaddr);
    sentinelFlushConfig();
    return C_OK;
}

/* Return non-zero if there was no SDOWN or ODOWN error associated to this
 * instance in the latest 'ms' milliseconds. */
int sentinelRedisInstanceNoDownFor(sentinelRedisInstance *ri, mstime_t ms) {
    mstime_t most_recent;

    most_recent = ri->s_down_since_time;
    if (ri->o_down_since_time > most_recent)
        most_recent = ri->o_down_since_time;
    return most_recent == 0 || (mstime() - most_recent) > ms;
}

/* Return the current primary address, that is, its address or the address
 * of the promoted replica if already operational. */
sentinelAddr *sentinelGetCurrentPrimaryAddress(sentinelRedisInstance *primary) {
    /* If we are failing over the primary, and the state is already
     * SENTINEL_FAILOVER_STATE_RECONF_REPLICAS or greater, it means that we
     * already have the new configuration epoch in the primary, and the
     * replica acknowledged the configuration switch. Advertise the new
     * address. */
    if ((primary->flags & SRI_FAILOVER_IN_PROGRESS) &&
        primary->promoted_replica &&
        primary->failover_state >= SENTINEL_FAILOVER_STATE_RECONF_REPLICAS)
    {
        return primary->promoted_replica->addr;
    } else {
        return primary->addr;
    }
}

/* This function sets the down_after_period field value in 'primary' to all
 * the replicas and sentinel instances connected to this primary. */
void sentinelPropagateDownAfterPeriod(sentinelRedisInstance *primary) {
    dictIterator *di;
    dictEntry *de;
    int j;
    dict *d[] = {primary->replicas, primary->sentinels, NULL};

    for (j = 0; d[j]; j++) {
        di = dictGetIterator(d[j]);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            ri->down_after_period = primary->down_after_period;
        }
        dictReleaseIterator(di);
    }
}

char *sentinelGetInstanceTypeString(sentinelRedisInstance *ri) {
    if (ri->flags & SRI_PRIMARY) return "primary";
    else if (ri->flags & SRI_REPLICA) return "replica";
    else if (ri->flags & SRI_SENTINEL) return "sentinel";
    else return "unknown";
}

/* This function is used in order to send commands to Redis instances: the
 * commands we send from Sentinel may be renamed, a common case is a primary
 * with CONFIG and REPLICAOF commands renamed for security concerns. In that
 * case we check the ri->renamed_command table (or if the instance is a replica,
 * we check the one of the primary), and map the command that we should send
 * to the set of renamed commads. However, if the command was not renamed,
 * we just return "command" itself. */
char *sentinelInstanceMapCommand(sentinelRedisInstance *ri, char *command) {
    sds sc = sdsnew(command);
    if (ri->primary) ri = ri->primary;
    char *retval = dictFetchValue(ri->renamed_commands, sc);
    sdsfree(sc);
    return retval ? retval : command;
}

/* ============================ Config handling ============================= */
char *sentinelHandleConfiguration(char **argv, int argc) {
    sentinelRedisInstance *ri;

    if (!strcasecmp(argv[0],"monitor") && argc == 5) {
        /* monitor <name> <host> <port> <quorum> */
        int quorum = atoi(argv[4]);

        if (quorum <= 0) return "Quorum must be 1 or greater.";
        if (createSentinelRedisInstance(argv[1],SRI_PRIMARY,argv[2],
                                        atoi(argv[3]),quorum,NULL) == NULL)
        {
            switch(errno) {
            case EBUSY: return "Duplicated primary name.";
            case ENOENT: return "Can't resolve primary instance hostname.";
            case EINVAL: return "Invalid port number";
            }
        }
    } else if (!strcasecmp(argv[0],"down-after-milliseconds") && argc == 3) {
        /* down-after-milliseconds <name> <milliseconds> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        ri->down_after_period = atoi(argv[2]);
        if (ri->down_after_period <= 0)
            return "negative or zero time parameter.";
        sentinelPropagateDownAfterPeriod(ri);
    } else if (!strcasecmp(argv[0],"failover-timeout") && argc == 3) {
        /* failover-timeout <name> <milliseconds> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        ri->failover_timeout = atoi(argv[2]);
        if (ri->failover_timeout <= 0)
            return "negative or zero time parameter.";
   } else if (!strcasecmp(argv[0],"parallel-syncs") && argc == 3) {
        /* parallel-syncs <name> <milliseconds> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        ri->parallel_syncs = atoi(argv[2]);
   } else if (!strcasecmp(argv[0],"notification-script") && argc == 3) {
        /* notification-script <name> <path> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        if (access(argv[2],X_OK) == -1)
            return "Notification script seems non existing or non executable.";
        ri->notification_script = sdsnew(argv[2]);
   } else if (!strcasecmp(argv[0],"client-reconfig-script") && argc == 3) {
        /* client-reconfig-script <name> <path> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        if (access(argv[2],X_OK) == -1)
            return "Client reconfiguration script seems non existing or "
                   "non executable.";
        ri->client_reconfig_script = sdsnew(argv[2]);
   } else if (!strcasecmp(argv[0],"auth-pass") && argc == 3) {
        /* auth-pass <name> <password> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        ri->auth_pass = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"current-epoch") && argc == 2) {
        /* current-epoch <epoch> */
        unsigned long long current_epoch = strtoull(argv[1],NULL,10);
        if (current_epoch > sentinel.current_epoch)
            sentinel.current_epoch = current_epoch;
    } else if (!strcasecmp(argv[0],"myid") && argc == 2) {
        if (strlen(argv[1]) != CONFIG_RUN_ID_SIZE)
            return "Malformed Sentinel id in myid option.";
        memcpy(sentinel.myid,argv[1],CONFIG_RUN_ID_SIZE);
    } else if (!strcasecmp(argv[0],"config-epoch") && argc == 3) {
        /* config-epoch <name> <epoch> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        ri->config_epoch = strtoull(argv[2],NULL,10);
        /* The following update of current_epoch is not really useful as
         * now the current epoch is persisted on the config file, but
         * we leave this check here for redundancy. */
        if (ri->config_epoch > sentinel.current_epoch)
            sentinel.current_epoch = ri->config_epoch;
    } else if (!strcasecmp(argv[0],"leader-epoch") && argc == 3) {
        /* leader-epoch <name> <epoch> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        ri->leader_epoch = strtoull(argv[2],NULL,10);
    } else if (!strcasecmp(argv[0],"known-replica") && argc == 4) {
        sentinelRedisInstance *replica;

        /* known-replica <name> <ip> <port> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        if ((replica = createSentinelRedisInstance(NULL,SRI_REPLICA,argv[2],
                    atoi(argv[3]), ri->quorum, ri)) == NULL)
        {
            return "Wrong hostname or port for replica.";
        }
    } else if (!strcasecmp(argv[0],"known-sentinel") &&
               (argc == 4 || argc == 5)) {
        sentinelRedisInstance *si;

        if (argc == 5) { /* Ignore the old form without runid. */
            /* known-sentinel <name> <ip> <port> [runid] */
            ri = sentinelGetPrimaryByName(argv[1]);
            if (!ri) return "No such primary with specified name.";
            if ((si = createSentinelRedisInstance(argv[4],SRI_SENTINEL,argv[2],
                        atoi(argv[3]), ri->quorum, ri)) == NULL)
            {
                return "Wrong hostname or port for sentinel.";
            }
            si->runid = sdsnew(argv[4]);
            sentinelTryConnectionSharing(si);
        }
    } else if (!strcasecmp(argv[0],"rename-command") && argc == 4) {
        /* rename-command <name> <command> <renamed-command> */
        ri = sentinelGetPrimaryByName(argv[1]);
        if (!ri) return "No such primary with specified name.";
        sds oldcmd = sdsnew(argv[2]);
        sds newcmd = sdsnew(argv[3]);
        if (dictAdd(ri->renamed_commands,oldcmd,newcmd) != DICT_OK) {
            sdsfree(oldcmd);
            sdsfree(newcmd);
            return "Same command renamed multiple times with rename-command.";
        }
    } else if (!strcasecmp(argv[0],"announce-ip") && argc == 2) {
        /* announce-ip <ip-address> */
        if (strlen(argv[1]))
            sentinel.announce_ip = sdsnew(argv[1]);
    } else if (!strcasecmp(argv[0],"announce-port") && argc == 2) {
        /* announce-port <port> */
        sentinel.announce_port = atoi(argv[1]);
    } else if (!strcasecmp(argv[0],"deny-scripts-reconfig") && argc == 2) {
        /* deny-scripts-reconfig <yes|no> */
        if ((sentinel.deny_scripts_reconfig = yesnotoi(argv[1])) == -1) {
            return "Please specify yes or no for the "
                   "deny-scripts-reconfig options.";
        }
    } else {
        return "Unrecognized sentinel configuration statement.";
    }
    return NULL;
}

/* Implements CONFIG REWRITE for "sentinel" option.
 * This is used not just to rewrite the configuration given by the user
 * (the configured primaries) but also in order to retain the state of
 * Sentinel across restarts: config epoch of primaries, associated replicas
 * and sentinel instances, and so forth. */
void rewriteConfigSentinelOption(struct rewriteConfigState *state) {
    dictIterator *di, *di2;
    dictEntry *de;
    sds line;

    /* sentinel unique ID. */
    line = sdscatprintf(sdsempty(), "sentinel myid %s", sentinel.myid);
    rewriteConfigRewriteLine(state,"sentinel",line,1);

    /* sentinel deny-scripts-reconfig. */
    line = sdscatprintf(sdsempty(), "sentinel deny-scripts-reconfig %s",
        sentinel.deny_scripts_reconfig ? "yes" : "no");
    rewriteConfigRewriteLine(state,"sentinel",line,
        sentinel.deny_scripts_reconfig != SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG);

    /* For every primary emit a "sentinel monitor" config entry. */
    di = dictGetIterator(sentinel.primaries);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *primary, *ri;
        sentinelAddr *primary_addr;

        /* sentinel monitor */
        primary = dictGetVal(de);
        primary_addr = sentinelGetCurrentPrimaryAddress(primary);
        line = sdscatprintf(sdsempty(),"sentinel monitor %s %s %d %d",
            primary->name, primary_addr->ip, primary_addr->port,
            primary->quorum);
        rewriteConfigRewriteLine(state,"sentinel",line,1);

        /* sentinel down-after-milliseconds */
        if (primary->down_after_period != SENTINEL_DEFAULT_DOWN_AFTER) {
            line = sdscatprintf(sdsempty(),
                "sentinel down-after-milliseconds %s %ld",
                primary->name, (long) primary->down_after_period);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel failover-timeout */
        if (primary->failover_timeout != SENTINEL_DEFAULT_FAILOVER_TIMEOUT) {
            line = sdscatprintf(sdsempty(),
                "sentinel failover-timeout %s %ld",
                primary->name, (long) primary->failover_timeout);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel parallel-syncs */
        if (primary->parallel_syncs != SENTINEL_DEFAULT_PARALLEL_SYNCS) {
            line = sdscatprintf(sdsempty(),
                "sentinel parallel-syncs %s %d",
                primary->name, primary->parallel_syncs);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel notification-script */
        if (primary->notification_script) {
            line = sdscatprintf(sdsempty(),
                "sentinel notification-script %s %s",
                primary->name, primary->notification_script);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel client-reconfig-script */
        if (primary->client_reconfig_script) {
            line = sdscatprintf(sdsempty(),
                "sentinel client-reconfig-script %s %s",
                primary->name, primary->client_reconfig_script);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel auth-pass */
        if (primary->auth_pass) {
            line = sdscatprintf(sdsempty(),
                "sentinel auth-pass %s %s",
                primary->name, primary->auth_pass);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel config-epoch */
        line = sdscatprintf(sdsempty(),
            "sentinel config-epoch %s %llu",
            primary->name, (unsigned long long) primary->config_epoch);
        rewriteConfigRewriteLine(state,"sentinel",line,1);

        /* sentinel leader-epoch */
        line = sdscatprintf(sdsempty(),
            "sentinel leader-epoch %s %llu",
            primary->name, (unsigned long long) primary->leader_epoch);
        rewriteConfigRewriteLine(state,"sentinel",line,1);

        /* sentinel known-replica */
        di2 = dictGetIterator(primary->replicas);
        while((de = dictNext(di2)) != NULL) {
            sentinelAddr *replica_addr;

            ri = dictGetVal(de);
            replica_addr = ri->addr;

            /* If primary_addr (obtained using sentinelGetCurrentPrimaryAddress()
             * so it may be the address of the promoted replica) is equal to this
             * replica's address, a failover is in progress and the replica was
             * already successfully promoted. So as the address of this replica
             * we use the old primary address instead. */
            if (sentinelAddrIsEqual(replica_addr,primary_addr))
                replica_addr = primary->addr;
            line = sdscatprintf(sdsempty(),
                "sentinel known-replica %s %s %d",
                primary->name, replica_addr->ip, replica_addr->port);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }
        dictReleaseIterator(di2);

        /* sentinel known-sentinel */
        di2 = dictGetIterator(primary->sentinels);
        while((de = dictNext(di2)) != NULL) {
            ri = dictGetVal(de);
            if (ri->runid == NULL) continue;
            line = sdscatprintf(sdsempty(),
                "sentinel known-sentinel %s %s %d %s",
                primary->name, ri->addr->ip, ri->addr->port, ri->runid);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }
        dictReleaseIterator(di2);

        /* sentinel rename-command */
        di2 = dictGetIterator(primary->renamed_commands);
        while((de = dictNext(di2)) != NULL) {
            sds oldname = dictGetKey(de);
            sds newname = dictGetVal(de);
            line = sdscatprintf(sdsempty(),
                "sentinel rename-command %s %s %s",
                primary->name, oldname, newname);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }
        dictReleaseIterator(di2);
    }

    /* sentinel current-epoch is a global state valid for all the primaries. */
    line = sdscatprintf(sdsempty(),
        "sentinel current-epoch %llu", (unsigned long long) sentinel.current_epoch);
    rewriteConfigRewriteLine(state,"sentinel",line,1);

    /* sentinel announce-ip. */
    if (sentinel.announce_ip) {
        line = sdsnew("sentinel announce-ip ");
        line = sdscatrepr(line, sentinel.announce_ip, sdslen(sentinel.announce_ip));
        rewriteConfigRewriteLine(state,"sentinel",line,1);
    }

    /* sentinel announce-port. */
    if (sentinel.announce_port) {
        line = sdscatprintf(sdsempty(),"sentinel announce-port %d",
                            sentinel.announce_port);
        rewriteConfigRewriteLine(state,"sentinel",line,1);
    }

    dictReleaseIterator(di);
}

/* This function uses the config rewriting Redis engine in order to persist
 * the state of the Sentinel in the current configuration file.
 *
 * Before returning the function calls fsync() against the generated
 * configuration file to make sure changes are committed to disk.
 *
 * On failure the function logs a warning on the Redis log. */
void sentinelFlushConfig(void) {
    int fd = -1;
    int saved_hz = server.hz;
    int rewrite_status;

    server.hz = CONFIG_DEFAULT_HZ;
    rewrite_status = rewriteConfig(server.configfile);
    server.hz = saved_hz;

    if (rewrite_status == -1) goto werr;
    if ((fd = open(server.configfile,O_RDONLY)) == -1) goto werr;
    if (fsync(fd) == -1) goto werr;
    if (close(fd) == EOF) goto werr;
    return;

werr:
    if (fd != -1) close(fd);
    serverLog(LL_WARNING,"WARNING: Sentinel was not able to save the new configuration on disk!!!: %s", strerror(errno));
}

/* ====================== hiredis connection handling ======================= */

/* Send the AUTH command with the specified primary password if needed.
 * Note that for replicas the password set for the primary is used.
 *
 * We don't check at all if the command was successfully transmitted
 * to the instance as if it fails Sentinel will detect the instance down,
 * will disconnect and reconnect the link and so forth. */
void sentinelSendAuthIfNeeded(sentinelRedisInstance *ri, redisAsyncContext *c) {
    char *auth_pass = (ri->flags & SRI_PRIMARY) ? ri->auth_pass :
                                                 ri->primary->auth_pass;

    if (auth_pass) {
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s",
            sentinelInstanceMapCommand(ri,"AUTH"),
            auth_pass) == C_OK) ri->link->pending_commands++;
    }
}

/* Use CLIENT SETNAME to name the connection in the Redis instance as
 * sentinel-<first_8_chars_of_runid>-<connection_type>
 * The connection type is "cmd" or "pubsub" as specified by 'type'.
 *
 * This makes it possible to list all the sentinel instances connected
 * to a Redis servewr with CLIENT LIST, grepping for a specific name format. */
void sentinelSetClientName(sentinelRedisInstance *ri, redisAsyncContext *c, char *type) {
    char name[64];

    snprintf(name,sizeof(name),"sentinel-%.8s-%s",sentinel.myid,type);
    if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri,
        "%s SETNAME %s",
        sentinelInstanceMapCommand(ri,"CLIENT"),
        name) == C_OK)
    {
        ri->link->pending_commands++;
    }
}

/* Create the async connections for the instance link if the link
 * is disconnected. Note that link->disconnected is true even if just
 * one of the two links (commands and pub/sub) is missing. */
void sentinelReconnectInstance(sentinelRedisInstance *ri) {
    if (ri->link->disconnected == 0) return;
    if (ri->addr->port == 0) return; /* port == 0 means invalid address. */
    instanceLink *link = ri->link;
    mstime_t now = mstime();

    if (now - ri->link->last_reconn_time < SENTINEL_PING_PERIOD) return;
    ri->link->last_reconn_time = now;

    /* Commands connection. */
    if (link->cc == NULL) {
        link->cc = redisAsyncConnectBind(ri->addr->ip,ri->addr->port,NET_FIRST_BIND_ADDR);
        if (link->cc->err) {
            sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #%s",
                link->cc->errstr);
            instanceLinkCloseConnection(link,link->cc);
        } else {
            link->pending_commands = 0;
            link->cc_conn_time = mstime();
            link->cc->data = link;
            redisAeAttach(server.el,link->cc);
            redisAsyncSetConnectCallback(link->cc,
                    sentinelLinkEstablishedCallback);
            redisAsyncSetDisconnectCallback(link->cc,
                    sentinelDisconnectCallback);
            sentinelSendAuthIfNeeded(ri,link->cc);
            sentinelSetClientName(ri,link->cc,"cmd");

            /* Send a PING ASAP when reconnecting. */
            sentinelSendPing(ri);
        }
    }
    /* Pub / Sub */
    if ((ri->flags & (SRI_PRIMARY|SRI_REPLICA)) && link->pc == NULL) {
        link->pc = redisAsyncConnectBind(ri->addr->ip,ri->addr->port,NET_FIRST_BIND_ADDR);
        if (link->pc->err) {
            sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #%s",
                link->pc->errstr);
            instanceLinkCloseConnection(link,link->pc);
        } else {
            int retval;

            link->pc_conn_time = mstime();
            link->pc->data = link;
            redisAeAttach(server.el,link->pc);
            redisAsyncSetConnectCallback(link->pc,
                    sentinelLinkEstablishedCallback);
            redisAsyncSetDisconnectCallback(link->pc,
                    sentinelDisconnectCallback);
            sentinelSendAuthIfNeeded(ri,link->pc);
            sentinelSetClientName(ri,link->pc,"pubsub");
            /* Now we subscribe to the Sentinels "Hello" channel. */
            retval = redisAsyncCommand(link->pc,
                sentinelReceiveHelloMessages, ri, "%s %s",
                sentinelInstanceMapCommand(ri,"SUBSCRIBE"),
                SENTINEL_HELLO_CHANNEL);
            if (retval != C_OK) {
                /* If we can't subscribe, the Pub/Sub connection is useless
                 * and we can simply disconnect it and try again. */
                instanceLinkCloseConnection(link,link->pc);
                return;
            }
        }
    }
    /* Clear the disconnected status only if we have both the connections
     * (or just the commands connection if this is a sentinel instance). */
    if (link->cc && (ri->flags & SRI_SENTINEL || link->pc))
        link->disconnected = 0;
}

/* ======================== Redis instances pinging  ======================== */

/* Return true if primary looks "sane", that is:
 * 1) It is actually a primary in the current configuration.
 * 2) It reports itself as a primary.
 * 3) It is not SDOWN or ODOWN.
 * 4) We obtained last INFO no more than two times the INFO period time ago. */
int sentinelPrimaryLooksSane(sentinelRedisInstance *primary) {
    return
        primary->flags & SRI_PRIMARY &&
        primary->role_reported == SRI_PRIMARY &&
        (primary->flags & (SRI_S_DOWN|SRI_O_DOWN)) == 0 &&
        (mstime() - primary->info_refresh) < SENTINEL_INFO_PERIOD*2;
}

/* Process the INFO output from primaries. */
void sentinelRefreshInstanceInfo(sentinelRedisInstance *ri, const char *info) {
    sds *lines;
    int numlines, j;
    int role = 0;

    /* cache full INFO output for instance */
    sdsfree(ri->info);
    ri->info = sdsnew(info);

    /* The following fields must be reset to a given value in the case they
     * are not found at all in the INFO output. */
    ri->primary_link_down_time = 0;

    /* Process line by line. */
    lines = sdssplitlen(info,strlen(info),"\r\n",2,&numlines);
    for (j = 0; j < numlines; j++) {
        sentinelRedisInstance *replica;
        sds l = lines[j];

        /* run_id:<40 hex chars>*/
        if (sdslen(l) >= 47 && !memcmp(l,"run_id:",7)) {
            if (ri->runid == NULL) {
                ri->runid = sdsnewlen(l+7,40);
            } else {
                if (strncmp(ri->runid,l+7,40) != 0) {
                    sentinelEvent(LL_NOTICE,"+reboot",ri,"%@");
                    sdsfree(ri->runid);
                    ri->runid = sdsnewlen(l+7,40);
                }
            }
        }

        /* old versions: replica0:<ip>,<port>,<state>
         * new versions: replica0:ip=127.0.0.1,port=9999,... */
        if ((ri->flags & SRI_PRIMARY) &&
            sdslen(l) >= 7 &&
            !memcmp(l,"replica",5) && isdigit(l[5]))
        {
            char *ip, *port, *end;

            if (strstr(l,"ip=") == NULL) {
                /* Old format. */
                ip = strchr(l,':'); if (!ip) continue;
                ip++; /* Now ip points to start of ip address. */
                port = strchr(ip,','); if (!port) continue;
                *port = '\0'; /* nul term for easy access. */
                port++; /* Now port points to start of port number. */
                end = strchr(port,','); if (!end) continue;
                *end = '\0'; /* nul term for easy access. */
            } else {
                /* New format. */
                ip = strstr(l,"ip="); if (!ip) continue;
                ip += 3; /* Now ip points to start of ip address. */
                port = strstr(l,"port="); if (!port) continue;
                port += 5; /* Now port points to start of port number. */
                /* Nul term both fields for easy access. */
                end = strchr(ip,','); if (end) *end = '\0';
                end = strchr(port,','); if (end) *end = '\0';
            }

            /* Check if we already have this replica into our table,
             * otherwise add it. */
            if (sentinelRedisInstanceLookupReplica(ri,ip,atoi(port)) == NULL) {
                if ((replica = createSentinelRedisInstance(NULL,SRI_REPLICA,ip,
                            atoi(port), ri->quorum, ri)) != NULL)
                {
                    sentinelEvent(LL_NOTICE,"+replica",replica,"%@");
                    sentinelFlushConfig();
                }
            }
        }

        /* primary_link_down_since_seconds:<seconds> */
        if (sdslen(l) >= 32 &&
            !memcmp(l,"primary_link_down_since_seconds",30))
        {
            ri->primary_link_down_time = strtoll(l+31,NULL,10)*1000;
        }

        /* role:<role> */
        if (!memcmp(l,"role:primary",11)) role = SRI_PRIMARY;
        else if (!memcmp(l,"role:replica",10)) role = SRI_REPLICA;

        if (role == SRI_REPLICA) {
            /* primary_host:<host> */
            if (sdslen(l) >= 12 && !memcmp(l,"primary_host:",12)) {
                if (ri->replica_primary_host == NULL ||
                    strcasecmp(l+12,ri->replica_primary_host))
                {
                    sdsfree(ri->replica_primary_host);
                    ri->replica_primary_host = sdsnew(l+12);
                    ri->replica_conf_change_time = mstime();
                }
            }

            /* primary_port:<port> */
            if (sdslen(l) >= 12 && !memcmp(l,"primary_port:",12)) {
                int replica_primary_port = atoi(l+12);

                if (ri->replica_primary_port != replica_primary_port) {
                    ri->replica_primary_port = replica_primary_port;
                    ri->replica_conf_change_time = mstime();
                }
            }

            /* primary_link_status:<status> */
            if (sdslen(l) >= 19 && !memcmp(l,"primary_link_status:",19)) {
                ri->replica_primary_link_status =
                    (strcasecmp(l+19,"up") == 0) ?
                    SENTINEL_PRIMARY_LINK_STATUS_UP :
                    SENTINEL_PRIMARY_LINK_STATUS_DOWN;
            }

            /* replica_priority:<priority> */
            if (sdslen(l) >= 15 && !memcmp(l,"replica_priority:",15))
                ri->replica_priority = atoi(l+15);

            /* replica_repl_offset:<offset> */
            if (sdslen(l) >= 18 && !memcmp(l,"replica_repl_offset:",18))
                ri->replica_repl_offset = strtoull(l+18,NULL,10);
        }
    }
    ri->info_refresh = mstime();
    sdsfreesplitres(lines,numlines);

    /* ---------------------------- Acting half -----------------------------
     * Some things will not happen if sentinel.tilt is true, but some will
     * still be processed. */

    /* Remember when the role changed. */
    if (role != ri->role_reported) {
        ri->role_reported_time = mstime();
        ri->role_reported = role;
        if (role == SRI_REPLICA) ri->replica_conf_change_time = mstime();
        /* Log the event with +role-change if the new role is coherent or
         * with -role-change if there is a mismatch with the current config. */
        sentinelEvent(LL_VERBOSE,
            ((ri->flags & (SRI_PRIMARY|SRI_REPLICA)) == role) ?
            "+role-change" : "-role-change",
            ri, "%@ new reported role is %s",
            role == SRI_PRIMARY ? "primary" : "replica",
            ri->flags & SRI_PRIMARY ? "primary" : "replica");
    }

    /* None of the following conditions are processed when in tilt mode, so
     * return asap. */
    if (sentinel.tilt) return;

    /* Handle primary -> replica role switch. */
    if ((ri->flags & SRI_PRIMARY) && role == SRI_REPLICA) {
        /* Nothing to do, but primaries claiming to be replicas are
         * considered to be unreachable by Sentinel, so eventually
         * a failover will be triggered. */
    }

    /* Handle replica -> primary role switch. */
    if ((ri->flags & SRI_REPLICA) && role == SRI_PRIMARY) {
        /* If this is a promoted replica we can change state to the
         * failover state machine. */
        if ((ri->flags & SRI_PROMOTED) &&
            (ri->primary->flags & SRI_FAILOVER_IN_PROGRESS) &&
            (ri->primary->failover_state ==
                SENTINEL_FAILOVER_STATE_WAIT_PROMOTION))
        {
            /* Now that we are sure the replica was reconfigured as a primary
             * set the primary configuration epoch to the epoch we won the
             * election to perform this failover. This will force the other
             * Sentinels to update their config (assuming there is not
             * a newer one already available). */
            ri->primary->config_epoch = ri->primary->failover_epoch;
            ri->primary->failover_state = SENTINEL_FAILOVER_STATE_RECONF_REPLICAS;
            ri->primary->failover_state_change_time = mstime();
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING,"+promoted-replica",ri,"%@");
            if (sentinel.simfailure_flags &
                SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION)
                sentinelSimFailureCrash();
            sentinelEvent(LL_WARNING,"+failover-state-reconf-replicas",
                ri->primary,"%@");
            sentinelCallClientReconfScript(ri->primary,SENTINEL_LEADER,
                "start",ri->primary->addr,ri->addr);
            sentinelForceHelloUpdateForPrimary(ri->primary);
        } else {
            /* A replica turned into a primary. We want to force our view and
             * reconfigure as replica. Wait some time after the change before
             * going forward, to receive new configs if any. */
            mstime_t wait_time = SENTINEL_PUBLISH_PERIOD*4;

            if (!(ri->flags & SRI_PROMOTED) &&
                 sentinelPrimaryLooksSane(ri->primary) &&
                 sentinelRedisInstanceNoDownFor(ri,wait_time) &&
                 mstime() - ri->role_reported_time > wait_time)
            {
                int retval = sentinelSendReplicaOf(ri,
                        ri->primary->addr->ip,
                        ri->primary->addr->port);
                if (retval == C_OK)
                    sentinelEvent(LL_NOTICE,"+convert-to-replica",ri,"%@");
            }
        }
    }

    /* Handle replicas replicating to a different primary address. */
    if ((ri->flags & SRI_REPLICA) &&
        role == SRI_REPLICA &&
        (ri->replica_primary_port != ri->primary->addr->port ||
         strcasecmp(ri->replica_primary_host,ri->primary->addr->ip)))
    {
        mstime_t wait_time = ri->primary->failover_timeout;

        /* Make sure the primary is sane before reconfiguring this instance
         * into a replica. */
        if (sentinelPrimaryLooksSane(ri->primary) &&
            sentinelRedisInstanceNoDownFor(ri,wait_time) &&
            mstime() - ri->replica_conf_change_time > wait_time)
        {
            int retval = sentinelSendReplicaOf(ri,
                    ri->primary->addr->ip,
                    ri->primary->addr->port);
            if (retval == C_OK)
                sentinelEvent(LL_NOTICE,"+fix-replica-config",ri,"%@");
        }
    }

    /* Detect if the replica that is in the process of being reconfigured
     * changed state. */
    if ((ri->flags & SRI_REPLICA) && role == SRI_REPLICA &&
        (ri->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)))
    {
        /* SRI_RECONF_SENT -> SRI_RECONF_INPROG. */
        if ((ri->flags & SRI_RECONF_SENT) &&
            ri->replica_primary_host &&
            strcmp(ri->replica_primary_host,
                    ri->primary->promoted_replica->addr->ip) == 0 &&
            ri->replica_primary_port == ri->primary->promoted_replica->addr->port)
        {
            ri->flags &= ~SRI_RECONF_SENT;
            ri->flags |= SRI_RECONF_INPROG;
            sentinelEvent(LL_NOTICE,"+replica-reconf-inprog",ri,"%@");
        }

        /* SRI_RECONF_INPROG -> SRI_RECONF_DONE */
        if ((ri->flags & SRI_RECONF_INPROG) &&
            ri->replica_primary_link_status == SENTINEL_PRIMARY_LINK_STATUS_UP)
        {
            ri->flags &= ~SRI_RECONF_INPROG;
            ri->flags |= SRI_RECONF_DONE;
            sentinelEvent(LL_NOTICE,"+replica-reconf-done",ri,"%@");
        }
    }
}

void sentinelInfoReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STRING)
        sentinelRefreshInstanceInfo(ri,r->str);
}

/* Just discard the reply. We use this when we are not monitoring the return
 * value of the command but its effects directly. */
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    instanceLink *link = c->data;
    UNUSED(reply);
    UNUSED(privdata);

    if (link) link->pending_commands--;
}

void sentinelPingReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STATUS ||
        r->type == REDIS_REPLY_ERROR) {
        /* Update the "instance available" field only if this is an
         * acceptable reply. */
        if (strncmp(r->str,"PONG",4) == 0 ||
            strncmp(r->str,"LOADING",7) == 0 ||
            strncmp(r->str,"PRIMARYDOWN",10) == 0)
        {
            link->last_avail_time = mstime();
            link->act_ping_time = 0; /* Flag the pong as received. */
        } else {
            /* Send a SCRIPT KILL command if the instance appears to be
             * down because of a busy script. */
            if (strncmp(r->str,"BUSY",4) == 0 &&
                (ri->flags & SRI_S_DOWN) &&
                !(ri->flags & SRI_SCRIPT_KILL_SENT))
            {
                if (redisAsyncCommand(ri->link->cc,
                        sentinelDiscardReplyCallback, ri,
                        "%s KILL",
                        sentinelInstanceMapCommand(ri,"SCRIPT")) == C_OK)
                {
                    ri->link->pending_commands++;
                }
                ri->flags |= SRI_SCRIPT_KILL_SENT;
            }
        }
    }
    link->last_pong_time = mstime();
}

/* This is called when we get the reply about the PUBLISH command we send
 * to the primary to advertise this sentinel. */
void sentinelPublishReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Only update pub_time if we actually published our message. Otherwise
     * we'll retry again in 100 milliseconds. */
    if (r->type != REDIS_REPLY_ERROR)
        ri->last_pub_time = mstime();
}

/* Process an hello message received via Pub/Sub in primary or replica instance,
 * or sent directly to this sentinel via the (fake) PUBLISH command of Sentinel.
 *
 * If the primary name specified in the message is not known, the message is
 * discarded. */
void sentinelProcessHelloMessage(char *hello, int hello_len) {
    /* Format is composed of 8 tokens:
     * 0=ip,1=port,2=runid,3=current_epoch,4=primary_name,
     * 5=primary_ip,6=primary_port,7=primary_config_epoch. */
    int numtokens, port, removed, primary_port;
    uint64_t current_epoch, primary_config_epoch;
    char **token = sdssplitlen(hello, hello_len, ",", 1, &numtokens);
    sentinelRedisInstance *si, *primary;

    if (numtokens == 8) {
        /* Obtain a reference to the primary this hello message is about */
        primary = sentinelGetPrimaryByName(token[4]);
        if (!primary) goto cleanup; /* Unknown primary, skip the message. */

        /* First, try to see if we already have this sentinel. */
        port = atoi(token[1]);
        primary_port = atoi(token[6]);
        si = getSentinelRedisInstanceByAddrAndRunID(
                        primary->sentinels,token[0],port,token[2]);
        current_epoch = strtoull(token[3],NULL,10);
        primary_config_epoch = strtoull(token[7],NULL,10);

        if (!si) {
            /* If not, remove all the sentinels that have the same runid
             * because there was an address change, and add the same Sentinel
             * with the new address back. */
            removed = removeMatchingSentinelFromPrimary(primary,token[2]);
            if (removed) {
                sentinelEvent(LL_NOTICE,"+sentinel-address-switch",primary,
                    "%@ ip %s port %d for %s", token[0],port,token[2]);
            } else {
                /* Check if there is another Sentinel with the same address this
                 * new one is reporting. What we do if this happens is to set its
                 * port to 0, to signal the address is invalid. We'll update it
                 * later if we get an HELLO message. */
                sentinelRedisInstance *other =
                    getSentinelRedisInstanceByAddrAndRunID(
                        primary->sentinels, token[0],port,NULL);
                if (other) {
                    sentinelEvent(LL_NOTICE,"+sentinel-invalid-addr",other,"%@");
                    other->addr->port = 0; /* It means: invalid address. */
                    sentinelUpdateSentinelAddressInAllPrimaries(other);
                }
            }

            /* Add the new sentinel. */
            si = createSentinelRedisInstance(token[2],SRI_SENTINEL,
                            token[0],port,primary->quorum,primary);

            if (si) {
                if (!removed) sentinelEvent(LL_NOTICE,"+sentinel",si,"%@");
                /* The runid is NULL after a new instance creation and
                 * for Sentinels we don't have a later chance to fill it,
                 * so do it now. */
                si->runid = sdsnew(token[2]);
                sentinelTryConnectionSharing(si);
                if (removed) sentinelUpdateSentinelAddressInAllPrimaries(si);
                sentinelFlushConfig();
            }
        }

        /* Update local current_epoch if received current_epoch is greater.*/
        if (current_epoch > sentinel.current_epoch) {
            sentinel.current_epoch = current_epoch;
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING,"+new-epoch",primary,"%llu",
                (unsigned long long) sentinel.current_epoch);
        }

        /* Update primary info if received configuration is newer. */
        if (si && primary->config_epoch < primary_config_epoch) {
            primary->config_epoch = primary_config_epoch;
            if (primary_port != primary->addr->port ||
                strcmp(primary->addr->ip, token[5]))
            {
                sentinelAddr *old_addr;

                sentinelEvent(LL_WARNING,"+config-update-from",si,"%@");
                sentinelEvent(LL_WARNING,"+switch-primary",
                    primary,"%s %s %d %s %d",
                    primary->name,
                    primary->addr->ip, primary->addr->port,
                    token[5], primary_port);

                old_addr = dupSentinelAddr(primary->addr);
                sentinelResetPrimaryAndChangeAddress(primary, token[5], primary_port);
                sentinelCallClientReconfScript(primary,
                    SENTINEL_OBSERVER,"start",
                    old_addr,primary->addr);
                releaseSentinelAddr(old_addr);
            }
        }

        /* Update the state of the Sentinel. */
        if (si) si->last_hello_time = mstime();
    }

cleanup:
    sdsfreesplitres(token,numtokens);
}


/* This is our Pub/Sub callback for the Hello channel. It's useful in order
 * to discover other sentinels attached at the same primary. */
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    redisReply *r;
    UNUSED(c);

    if (!reply || !ri) return;
    r = reply;

    /* Update the last activity in the pubsub channel. Note that since we
     * receive our messages as well this timestamp can be used to detect
     * if the link is probably disconnected even if it seems otherwise. */
    ri->link->pc_last_activity = mstime();

    /* Sanity check in the reply we expect, so that the code that follows
     * can avoid to check for details. */
    if (r->type != REDIS_REPLY_ARRAY ||
        r->elements != 3 ||
        r->element[0]->type != REDIS_REPLY_STRING ||
        r->element[1]->type != REDIS_REPLY_STRING ||
        r->element[2]->type != REDIS_REPLY_STRING ||
        strcmp(r->element[0]->str,"message") != 0) return;

    /* We are not interested in meeting ourselves */
    if (strstr(r->element[2]->str,sentinel.myid) != NULL) return;

    sentinelProcessHelloMessage(r->element[2]->str, r->element[2]->len);
}

/* Send an "Hello" message via Pub/Sub to the specified 'ri' Redis
 * instance in order to broadcast the current configuration for this
 * primary, and to advertise the existence of this Sentinel at the same time.
 *
 * The message has the following format:
 *
 * sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
 * primary_name,primary_ip,primary_port,primary_config_epoch.
 *
 * Returns C_OK if the PUBLISH was queued correctly, otherwise
 * C_ERR is returned. */
int sentinelSendHello(sentinelRedisInstance *ri) {
    char ip[NET_IP_STR_LEN];
    char payload[NET_IP_STR_LEN+1024];
    int retval;
    char *announce_ip;
    int announce_port;
    sentinelRedisInstance *primary = (ri->flags & SRI_PRIMARY) ? ri : ri->primary;
    sentinelAddr *primary_addr = sentinelGetCurrentPrimaryAddress(primary);

    if (ri->link->disconnected) return C_ERR;

    /* Use the specified announce address if specified, otherwise try to
     * obtain our own IP address. */
    if (sentinel.announce_ip) {
        announce_ip = sentinel.announce_ip;
    } else {
        if (anetSockName(ri->link->cc->c.fd,ip,sizeof(ip),NULL) == -1)
            return C_ERR;
        announce_ip = ip;
    }
    announce_port = sentinel.announce_port ?
                    sentinel.announce_port : server.port;

    /* Format and send the Hello message. */
    snprintf(payload,sizeof(payload),
        "%s,%d,%s,%llu," /* Info about this sentinel. */
        "%s,%s,%d,%llu", /* Info about current primary. */
        announce_ip, announce_port, sentinel.myid,
        (unsigned long long) sentinel.current_epoch,
        /* --- */
        primary->name,primary_addr->ip,primary_addr->port,
        (unsigned long long) primary->config_epoch);
    retval = redisAsyncCommand(ri->link->cc,
        sentinelPublishReplyCallback, ri, "%s %s %s",
        sentinelInstanceMapCommand(ri,"PUBLISH"),
        SENTINEL_HELLO_CHANNEL,payload);
    if (retval != C_OK) return C_ERR;
    ri->link->pending_commands++;
    return C_OK;
}

/* Reset last_pub_time in all the instances in the specified dictionary
 * in order to force the delivery of an Hello update ASAP. */
void sentinelForceHelloUpdateDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->last_pub_time >= (SENTINEL_PUBLISH_PERIOD+1))
            ri->last_pub_time -= (SENTINEL_PUBLISH_PERIOD+1);
    }
    dictReleaseIterator(di);
}

/* This function forces the delivery of an "Hello" message (see
 * sentinelSendHello() top comment for further information) to all the Redis
 * and Sentinel instances related to the specified 'primary'.
 *
 * It is technically not needed since we send an update to every instance
 * with a period of SENTINEL_PUBLISH_PERIOD milliseconds, however when a
 * Sentinel upgrades a configuration it is a good idea to deliever an update
 * to the other Sentinels ASAP. */
int sentinelForceHelloUpdateForPrimary(sentinelRedisInstance *primary) {
    if (!(primary->flags & SRI_PRIMARY)) return C_ERR;
    if (primary->last_pub_time >= (SENTINEL_PUBLISH_PERIOD+1))
        primary->last_pub_time -= (SENTINEL_PUBLISH_PERIOD+1);
    sentinelForceHelloUpdateDictOfRedisInstances(primary->sentinels);
    sentinelForceHelloUpdateDictOfRedisInstances(primary->replicas);
    return C_OK;
}

/* Send a PING to the specified instance and refresh the act_ping_time
 * if it is zero (that is, if we received a pong for the previous ping).
 *
 * On error zero is returned, and we can't consider the PING command
 * queued in the connection. */
int sentinelSendPing(sentinelRedisInstance *ri) {
    int retval = redisAsyncCommand(ri->link->cc,
        sentinelPingReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"PING"));
    if (retval == C_OK) {
        ri->link->pending_commands++;
        ri->link->last_ping_time = mstime();
        /* We update the active ping time only if we received the pong for
         * the previous ping, otherwise we are technically waiting since the
         * first ping that did not receive a reply. */
        if (ri->link->act_ping_time == 0)
            ri->link->act_ping_time = ri->link->last_ping_time;
        return 1;
    } else {
        return 0;
    }
}

/* Send periodic PING, INFO, and PUBLISH to the Hello channel to
 * the specified primary or replica instance. */
void sentinelSendPeriodicCommands(sentinelRedisInstance *ri) {
    mstime_t now = mstime();
    mstime_t info_period, ping_period;
    int retval;

    /* Return ASAP if we have already a PING or INFO already pending, or
     * in the case the instance is not properly connected. */
    if (ri->link->disconnected) return;

    /* For INFO, PING, PUBLISH that are not critical commands to send we
     * also have a limit of SENTINEL_MAX_PENDING_COMMANDS. We don't
     * want to use a lot of memory just because a link is not working
     * properly (note that anyway there is a redundant protection about this,
     * that is, the link will be disconnected and reconnected if a long
     * timeout condition is detected. */
    if (ri->link->pending_commands >=
        SENTINEL_MAX_PENDING_COMMANDS * ri->link->refcount) return;

    /* If this is a replica of a primary in O_DOWN condition we start sending
     * it INFO every second, instead of the usual SENTINEL_INFO_PERIOD
     * period. In this state we want to closely monitor replicas in case they
     * are turned into primaries by another Sentinel, or by the sysadmin.
     *
     * Similarly we monitor the INFO output more often if the replica reports
     * to be disconnected from the primary, so that we can have a fresh
     * disconnection time figure. */
    if ((ri->flags & SRI_REPLICA) &&
        ((ri->primary->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS)) ||
         (ri->primary_link_down_time != 0)))
    {
        info_period = 1000;
    } else {
        info_period = SENTINEL_INFO_PERIOD;
    }

    /* We ping instances every time the last received pong is older than
     * the configured 'down-after-milliseconds' time, but every second
     * anyway if 'down-after-milliseconds' is greater than 1 second. */
    ping_period = ri->down_after_period;
    if (ping_period > SENTINEL_PING_PERIOD) ping_period = SENTINEL_PING_PERIOD;

    /* Send INFO to primaries and replicas, not sentinels. */
    if ((ri->flags & SRI_SENTINEL) == 0 &&
        (ri->info_refresh == 0 ||
        (now - ri->info_refresh) > info_period))
    {
        retval = redisAsyncCommand(ri->link->cc,
            sentinelInfoReplyCallback, ri, "%s",
            sentinelInstanceMapCommand(ri,"INFO"));
        if (retval == C_OK) ri->link->pending_commands++;
    }

    /* Send PING to all the three kinds of instances. */
    if ((now - ri->link->last_pong_time) > ping_period &&
               (now - ri->link->last_ping_time) > ping_period/2) {
        sentinelSendPing(ri);
    }

    /* PUBLISH hello messages to all the three kinds of instances. */
    if ((now - ri->last_pub_time) > SENTINEL_PUBLISH_PERIOD) {
        sentinelSendHello(ri);
    }
}

/* =========================== SENTINEL command ============================= */

const char *sentinelFailoverStateStr(int state) {
    switch(state) {
    case SENTINEL_FAILOVER_STATE_NONE: return "none";
    case SENTINEL_FAILOVER_STATE_WAIT_START: return "wait_start";
    case SENTINEL_FAILOVER_STATE_SELECT_REPLICA: return "select_replica";
    case SENTINEL_FAILOVER_STATE_SEND_REPLICAOF_NOONE: return "send_replicaof_noone";
    case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION: return "wait_promotion";
    case SENTINEL_FAILOVER_STATE_RECONF_REPLICAS: return "reconf_replicas";
    case SENTINEL_FAILOVER_STATE_UPDATE_CONFIG: return "update_config";
    default: return "unknown";
    }
}

/* Redis instance to Redis protocol representation. */
void addReplySentinelRedisInstance(client *c, sentinelRedisInstance *ri) {
    char *flags = sdsempty();
    void *mbl;
    int fields = 0;

    mbl = addDeferredMultiBulkLength(c);

    addReplyBulkCString(c,"name");
    addReplyBulkCString(c,ri->name);
    fields++;

    addReplyBulkCString(c,"ip");
    addReplyBulkCString(c,ri->addr->ip);
    fields++;

    addReplyBulkCString(c,"port");
    addReplyBulkLongLong(c,ri->addr->port);
    fields++;

    addReplyBulkCString(c,"runid");
    addReplyBulkCString(c,ri->runid ? ri->runid : "");
    fields++;

    addReplyBulkCString(c,"flags");
    if (ri->flags & SRI_S_DOWN) flags = sdscat(flags,"s_down,");
    if (ri->flags & SRI_O_DOWN) flags = sdscat(flags,"o_down,");
    if (ri->flags & SRI_PRIMARY) flags = sdscat(flags,"primary,");
    if (ri->flags & SRI_REPLICA) flags = sdscat(flags,"replica,");
    if (ri->flags & SRI_SENTINEL) flags = sdscat(flags,"sentinel,");
    if (ri->link->disconnected) flags = sdscat(flags,"disconnected,");
    if (ri->flags & SRI_PRIMARY_DOWN) flags = sdscat(flags,"primary_down,");
    if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
        flags = sdscat(flags,"failover_in_progress,");
    if (ri->flags & SRI_PROMOTED) flags = sdscat(flags,"promoted,");
    if (ri->flags & SRI_RECONF_SENT) flags = sdscat(flags,"reconf_sent,");
    if (ri->flags & SRI_RECONF_INPROG) flags = sdscat(flags,"reconf_inprog,");
    if (ri->flags & SRI_RECONF_DONE) flags = sdscat(flags,"reconf_done,");

    if (sdslen(flags) != 0) sdsrange(flags,0,-2); /* remove last "," */
    addReplyBulkCString(c,flags);
    sdsfree(flags);
    fields++;

    addReplyBulkCString(c,"link-pending-commands");
    addReplyBulkLongLong(c,ri->link->pending_commands);
    fields++;

    addReplyBulkCString(c,"link-refcount");
    addReplyBulkLongLong(c,ri->link->refcount);
    fields++;

    if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
        addReplyBulkCString(c,"failover-state");
        addReplyBulkCString(c,(char*)sentinelFailoverStateStr(ri->failover_state));
        fields++;
    }

    addReplyBulkCString(c,"last-ping-sent");
    addReplyBulkLongLong(c,
        ri->link->act_ping_time ? (mstime() - ri->link->act_ping_time) : 0);
    fields++;

    addReplyBulkCString(c,"last-ok-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->link->last_avail_time);
    fields++;

    addReplyBulkCString(c,"last-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->link->last_pong_time);
    fields++;

    if (ri->flags & SRI_S_DOWN) {
        addReplyBulkCString(c,"s-down-time");
        addReplyBulkLongLong(c,mstime()-ri->s_down_since_time);
        fields++;
    }

    if (ri->flags & SRI_O_DOWN) {
        addReplyBulkCString(c,"o-down-time");
        addReplyBulkLongLong(c,mstime()-ri->o_down_since_time);
        fields++;
    }

    addReplyBulkCString(c,"down-after-milliseconds");
    addReplyBulkLongLong(c,ri->down_after_period);
    fields++;

    /* Primaries and Replicas */
    if (ri->flags & (SRI_PRIMARY|SRI_REPLICA)) {
        addReplyBulkCString(c,"info-refresh");
        addReplyBulkLongLong(c,mstime() - ri->info_refresh);
        fields++;

        addReplyBulkCString(c,"role-reported");
        addReplyBulkCString(c, (ri->role_reported == SRI_PRIMARY) ? "primary" :
                                                                   "replica");
        fields++;

        addReplyBulkCString(c,"role-reported-time");
        addReplyBulkLongLong(c,mstime() - ri->role_reported_time);
        fields++;
    }

    /* Only primaries */
    if (ri->flags & SRI_PRIMARY) {
        addReplyBulkCString(c,"config-epoch");
        addReplyBulkLongLong(c,ri->config_epoch);
        fields++;

        addReplyBulkCString(c,"num-replicas");
        addReplyBulkLongLong(c,dictSize(ri->replicas));
        fields++;

        addReplyBulkCString(c,"num-other-sentinels");
        addReplyBulkLongLong(c,dictSize(ri->sentinels));
        fields++;

        addReplyBulkCString(c,"quorum");
        addReplyBulkLongLong(c,ri->quorum);
        fields++;

        addReplyBulkCString(c,"failover-timeout");
        addReplyBulkLongLong(c,ri->failover_timeout);
        fields++;

        addReplyBulkCString(c,"parallel-syncs");
        addReplyBulkLongLong(c,ri->parallel_syncs);
        fields++;

        if (ri->notification_script) {
            addReplyBulkCString(c,"notification-script");
            addReplyBulkCString(c,ri->notification_script);
            fields++;
        }

        if (ri->client_reconfig_script) {
            addReplyBulkCString(c,"client-reconfig-script");
            addReplyBulkCString(c,ri->client_reconfig_script);
            fields++;
        }
    }

    /* Only replicas */
    if (ri->flags & SRI_REPLICA) {
        addReplyBulkCString(c,"primary-link-down-time");
        addReplyBulkLongLong(c,ri->primary_link_down_time);
        fields++;

        addReplyBulkCString(c,"primary-link-status");
        addReplyBulkCString(c,
            (ri->replica_primary_link_status == SENTINEL_PRIMARY_LINK_STATUS_UP) ?
            "ok" : "err");
        fields++;

        addReplyBulkCString(c,"primary-host");
        addReplyBulkCString(c,
            ri->replica_primary_host ? ri->replica_primary_host : "?");
        fields++;

        addReplyBulkCString(c,"primary-port");
        addReplyBulkLongLong(c,ri->replica_primary_port);
        fields++;

        addReplyBulkCString(c,"replica-priority");
        addReplyBulkLongLong(c,ri->replica_priority);
        fields++;

        addReplyBulkCString(c,"replica-repl-offset");
        addReplyBulkLongLong(c,ri->replica_repl_offset);
        fields++;
    }

    /* Only sentinels */
    if (ri->flags & SRI_SENTINEL) {
        addReplyBulkCString(c,"last-hello-message");
        addReplyBulkLongLong(c,mstime() - ri->last_hello_time);
        fields++;

        addReplyBulkCString(c,"voted-leader");
        addReplyBulkCString(c,ri->leader ? ri->leader : "?");
        fields++;

        addReplyBulkCString(c,"voted-leader-epoch");
        addReplyBulkLongLong(c,ri->leader_epoch);
        fields++;
    }

    setDeferredMultiBulkLength(c,mbl,fields*2);
}

/* Output a number of instances contained inside a dictionary as
 * Redis protocol. */
void addReplyDictOfRedisInstances(client *c, dict *instances) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    addReplyMultiBulkLen(c,dictSize(instances));
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplySentinelRedisInstance(c,ri);
    }
    dictReleaseIterator(di);
}

/* Lookup the named primary into sentinel.primaries.
 * If the primary is not found reply to the client with an error and returns
 * NULL. */
sentinelRedisInstance *sentinelGetPrimaryByNameOrReplyError(client *c,
                        robj *name)
{
    sentinelRedisInstance *ri;

    ri = dictFetchValue(sentinel.primaries,name->ptr);
    if (!ri) {
        addReplyError(c,"No such primary with that name");
        return NULL;
    }
    return ri;
}

#define SENTINEL_ISQR_OK 0
#define SENTINEL_ISQR_NOQUORUM (1<<0)
#define SENTINEL_ISQR_NOAUTH (1<<1)
int sentinelIsQuorumReachable(sentinelRedisInstance *primary, int *usableptr) {
    dictIterator *di;
    dictEntry *de;
    int usable = 1; /* Number of usable Sentinels. Init to 1 to count myself. */
    int result = SENTINEL_ISQR_OK;
    int voters = dictSize(primary->sentinels)+1; /* Known Sentinels + myself. */

    di = dictGetIterator(primary->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->flags & (SRI_S_DOWN|SRI_O_DOWN)) continue;
        usable++;
    }
    dictReleaseIterator(di);

    if (usable < (int)primary->quorum) result |= SENTINEL_ISQR_NOQUORUM;
    if (usable < voters/2+1) result |= SENTINEL_ISQR_NOAUTH;
    if (usableptr) *usableptr = usable;
    return result;
}

void sentinelCommand(client *c) {
    if (!strcasecmp(c->argv[1]->ptr,"primaries")) {
        /* SENTINEL PRIMARIES */
        if (c->argc != 2) goto numargserr;
        addReplyDictOfRedisInstances(c,sentinel.primaries);
    } else if (!strcasecmp(c->argv[1]->ptr,"primary")) {
        /* SENTINEL PRIMARY <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetPrimaryByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        addReplySentinelRedisInstance(c,ri);
    } else if (!strcasecmp(c->argv[1]->ptr,"replicas")) {
        /* SENTINEL REPLICAS <primary-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetPrimaryByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c,ri->replicas);
    } else if (!strcasecmp(c->argv[1]->ptr,"sentinels")) {
        /* SENTINEL SENTINELS <primary-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetPrimaryByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c,ri->sentinels);
    } else if (!strcasecmp(c->argv[1]->ptr,"is-primary-down-by-addr")) {
        /* SENTINEL IS-PRIMARY-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
         *
         * Arguments:
         *
         * ip and port are the ip and port of the primary we want to be
         * checked by Sentinel. Note that the command will not check by
         * name but just by primary, in theory different Sentinels may monitor
         * differnet primaries with the same name.
         *
         * current-epoch is needed in order to understand if we are allowed
         * to vote for a failover leader or not. Each Sentinel can vote just
         * one time per epoch.
         *
         * runid is "*" if we are not seeking for a vote from the Sentinel
         * in order to elect the failover leader. Otherwise it is set to the
         * runid we want the Sentinel to vote if it did not already voted.
         */
        sentinelRedisInstance *ri;
        long long req_epoch;
        uint64_t leader_epoch = 0;
        char *leader = NULL;
        long port;
        int isdown = 0;

        if (c->argc != 6) goto numargserr;
        if (getLongFromObjectOrReply(c,c->argv[3],&port,NULL) != C_OK ||
            getLongLongFromObjectOrReply(c,c->argv[4],&req_epoch,NULL)
                                                              != C_OK)
            return;
        ri = getSentinelRedisInstanceByAddrAndRunID(sentinel.primaries,
            c->argv[2]->ptr,port,NULL);

        /* It exists? Is actually a primary? Is subjectively down? It's down.
         * Note: if we are in tilt mode we always reply with "0". */
        if (!sentinel.tilt && ri && (ri->flags & SRI_S_DOWN) &&
                                    (ri->flags & SRI_PRIMARY))
            isdown = 1;

        /* Vote for the primary (or fetch the previous vote) if the request
         * includes a runid, otherwise the sender is not seeking for a vote. */
        if (ri && ri->flags & SRI_PRIMARY && strcasecmp(c->argv[5]->ptr,"*")) {
            leader = sentinelVoteLeader(ri,(uint64_t)req_epoch,
                                            c->argv[5]->ptr,
                                            &leader_epoch);
        }

        /* Reply with a three-elements multi-bulk reply:
         * down state, leader, vote epoch. */
        addReplyMultiBulkLen(c,3);
        addReply(c, isdown ? shared.cone : shared.czero);
        addReplyBulkCString(c, leader ? leader : "*");
        addReplyLongLong(c, (long long)leader_epoch);
        if (leader) sdsfree(leader);
    } else if (!strcasecmp(c->argv[1]->ptr,"reset")) {
        /* SENTINEL RESET <pattern> */
        if (c->argc != 3) goto numargserr;
        addReplyLongLong(c,sentinelResetPrimariesByPattern(c->argv[2]->ptr,SENTINEL_GENERATE_EVENT));
    } else if (!strcasecmp(c->argv[1]->ptr,"get-primary-addr-by-name")) {
        /* SENTINEL GET-PRIMARY-ADDR-BY-NAME <primary-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        ri = sentinelGetPrimaryByName(c->argv[2]->ptr);
        if (ri == NULL) {
            addReply(c,shared.nullmultibulk);
        } else {
            sentinelAddr *addr = sentinelGetCurrentPrimaryAddress(ri);

            addReplyMultiBulkLen(c,2);
            addReplyBulkCString(c,addr->ip);
            addReplyBulkLongLong(c,addr->port);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"failover")) {
        /* SENTINEL FAILOVER <primary-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetPrimaryByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
            addReplySds(c,sdsnew("-INPROG Failover already in progress\r\n"));
            return;
        }
        if (sentinelSelectReplica(ri) == NULL) {
            addReplySds(c,sdsnew("-NOGOODREPLICA No suitable replica to promote\r\n"));
            return;
        }
        serverLog(LL_WARNING,"Executing user requested FAILOVER of '%s'",
            ri->name);
        sentinelStartFailover(ri);
        ri->flags |= SRI_FORCE_FAILOVER;
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"pending-scripts")) {
        /* SENTINEL PENDING-SCRIPTS */

        if (c->argc != 2) goto numargserr;
        sentinelPendingScriptsCommand(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"monitor")) {
        /* SENTINEL MONITOR <name> <ip> <port> <quorum> */
        sentinelRedisInstance *ri;
        long quorum, port;
        char ip[NET_IP_STR_LEN];

        if (c->argc != 6) goto numargserr;
        if (getLongFromObjectOrReply(c,c->argv[5],&quorum,"Invalid quorum")
            != C_OK) return;
        if (getLongFromObjectOrReply(c,c->argv[4],&port,"Invalid port")
            != C_OK) return;

        if (quorum <= 0) {
            addReplyError(c, "Quorum must be 1 or greater.");
            return;
        }

        /* Make sure the IP field is actually a valid IP before passing it
         * to createSentinelRedisInstance(), otherwise we may trigger a
         * DNS lookup at runtime. */
        if (anetResolveIP(NULL,c->argv[3]->ptr,ip,sizeof(ip)) == ANET_ERR) {
            addReplyError(c,"Invalid IP address specified");
            return;
        }

        /* Parameters are valid. Try to create the primary instance. */
        ri = createSentinelRedisInstance(c->argv[2]->ptr,SRI_PRIMARY,
                c->argv[3]->ptr,port,quorum,NULL);
        if (ri == NULL) {
            switch(errno) {
            case EBUSY:
                addReplyError(c,"Duplicated primary name");
                break;
            case EINVAL:
                addReplyError(c,"Invalid port number");
                break;
            default:
                addReplyError(c,"Unspecified error adding the instance");
                break;
            }
        } else {
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING,"+monitor",ri,"%@ quorum %d",ri->quorum);
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"flushconfig")) {
        if (c->argc != 2) goto numargserr;
        sentinelFlushConfig();
        addReply(c,shared.ok);
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"remove")) {
        /* SENTINEL REMOVE <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetPrimaryByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        sentinelEvent(LL_WARNING,"-monitor",ri,"%@");
        dictDelete(sentinel.primaries,c->argv[2]->ptr);
        sentinelFlushConfig();
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"ckquorum")) {
        /* SENTINEL CKQUORUM <name> */
        sentinelRedisInstance *ri;
        int usable;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetPrimaryByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        int result = sentinelIsQuorumReachable(ri,&usable);
        if (result == SENTINEL_ISQR_OK) {
            addReplySds(c, sdscatfmt(sdsempty(),
                "+OK %i usable Sentinels. Quorum and failover authorization "
                "can be reached\r\n",usable));
        } else {
            sds e = sdscatfmt(sdsempty(),
                "-NOQUORUM %i usable Sentinels. ",usable);
            if (result & SENTINEL_ISQR_NOQUORUM)
                e = sdscat(e,"Not enough available Sentinels to reach the"
                             " specified quorum for this primary");
            if (result & SENTINEL_ISQR_NOAUTH) {
                if (result & SENTINEL_ISQR_NOQUORUM) e = sdscat(e,". ");
                e = sdscat(e, "Not enough available Sentinels to reach the"
                              " majority and authorize a failover");
            }
            e = sdscat(e,"\r\n");
            addReplySds(c,e);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"set")) {
        if (c->argc < 3) goto numargserr;
        sentinelSetCommand(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"info-cache")) {
        /* SENTINEL INFO-CACHE <name> */
        if (c->argc < 2) goto numargserr;
        mstime_t now = mstime();

        /* Create an ad-hoc dictionary type so that we can iterate
         * a dictionary composed of just the primary groups the user
         * requested. */
        dictType copy_keeper = instancesDictType;
        copy_keeper.valDestructor = NULL;
        dict *primaries_local = sentinel.primaries;
        if (c->argc > 2) {
            primaries_local = dictCreate(&copy_keeper, NULL);

            for (int i = 2; i < c->argc; i++) {
                sentinelRedisInstance *ri;
                ri = sentinelGetPrimaryByName(c->argv[i]->ptr);
                if (!ri) continue; /* ignore non-existing names */
                dictAdd(primaries_local, ri->name, ri);
            }
        }

        /* Reply format:
         *   1.) primary name
         *   2.) 1.) info from primary
         *       2.) info from replica
         *       ...
         *   3.) other primary name
         *   ...
         */
        addReplyMultiBulkLen(c,dictSize(primaries_local) * 2);

        dictIterator  *di;
        dictEntry *de;
        di = dictGetIterator(primaries_local);
        while ((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            addReplyBulkCBuffer(c,ri->name,strlen(ri->name));
            addReplyMultiBulkLen(c,dictSize(ri->replicas) + 1); /* +1 for self */
            addReplyMultiBulkLen(c,2);
            addReplyLongLong(c, now - ri->info_refresh);
            if (ri->info)
                addReplyBulkCBuffer(c,ri->info,sdslen(ri->info));
            else
                addReply(c,shared.nullbulk);

            dictIterator *sdi;
            dictEntry *sde;
            sdi = dictGetIterator(ri->replicas);
            while ((sde = dictNext(sdi)) != NULL) {
                sentinelRedisInstance *sri = dictGetVal(sde);
                addReplyMultiBulkLen(c,2);
                addReplyLongLong(c, now - sri->info_refresh);
                if (sri->info)
                    addReplyBulkCBuffer(c,sri->info,sdslen(sri->info));
                else
                    addReply(c,shared.nullbulk);
            }
            dictReleaseIterator(sdi);
        }
        dictReleaseIterator(di);
        if (primaries_local != sentinel.primaries) dictRelease(primaries_local);
    } else if (!strcasecmp(c->argv[1]->ptr,"simulate-failure")) {
        /* SENTINEL SIMULATE-FAILURE <flag> <flag> ... <flag> */
        int j;

        sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
        for (j = 2; j < c->argc; j++) {
            if (!strcasecmp(c->argv[j]->ptr,"crash-after-election")) {
                sentinel.simfailure_flags |=
                    SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION;
                serverLog(LL_WARNING,"Failure simulation: this Sentinel "
                    "will crash after being successfully elected as failover "
                    "leader");
            } else if (!strcasecmp(c->argv[j]->ptr,"crash-after-promotion")) {
                sentinel.simfailure_flags |=
                    SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION;
                serverLog(LL_WARNING,"Failure simulation: this Sentinel "
                    "will crash after promoting the selected replica to primary");
            } else if (!strcasecmp(c->argv[j]->ptr,"help")) {
                addReplyMultiBulkLen(c,2);
                addReplyBulkCString(c,"crash-after-election");
                addReplyBulkCString(c,"crash-after-promotion");
            } else {
                addReplyError(c,"Unknown failure simulation specified");
                return;
            }
        }
        addReply(c,shared.ok);
    } else {
        addReplyErrorFormat(c,"Unknown sentinel subcommand '%s'",
                               (char*)c->argv[1]->ptr);
    }
    return;

numargserr:
    addReplyErrorFormat(c,"Wrong number of arguments for 'sentinel %s'",
                          (char*)c->argv[1]->ptr);
}

#define info_section_from_redis(section_name) do { \
    if (defsections || allsections || !strcasecmp(section,section_name)) { \
        sds redissection; \
        if (sections++) info = sdscat(info,"\r\n"); \
        redissection = genRedisInfoString(section_name); \
        info = sdscatlen(info,redissection,sdslen(redissection)); \
        sdsfree(redissection); \
    } \
} while(0)

/* SENTINEL INFO [section] */
void sentinelInfoCommand(client *c) {
    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    }

    int defsections = 0, allsections = 0;
    char *section = c->argc == 2 ? c->argv[1]->ptr : NULL;
    if (section) {
        allsections = !strcasecmp(section,"all");
        defsections = !strcasecmp(section,"default");
    } else {
        defsections = 1;
    }

    int sections = 0;
    sds info = sdsempty();

    info_section_from_redis("server");
    info_section_from_redis("clients");
    info_section_from_redis("cpu");
    info_section_from_redis("stats");

    if (defsections || allsections || !strcasecmp(section,"sentinel")) {
        dictIterator *di;
        dictEntry *de;
        int primary_id = 0;

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Sentinel\r\n"
            "sentinel_primaries:%lu\r\n"
            "sentinel_tilt:%d\r\n"
            "sentinel_running_scripts:%d\r\n"
            "sentinel_scripts_queue_length:%ld\r\n"
            "sentinel_simulate_failure_flags:%lu\r\n",
            dictSize(sentinel.primaries),
            sentinel.tilt,
            sentinel.running_scripts,
            listLength(sentinel.scripts_queue),
            sentinel.simfailure_flags);

        di = dictGetIterator(sentinel.primaries);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            char *status = "ok";

            if (ri->flags & SRI_O_DOWN) status = "odown";
            else if (ri->flags & SRI_S_DOWN) status = "sdown";
            info = sdscatprintf(info,
                "primary%d:name=%s,status=%s,address=%s:%d,"
                "replicas=%lu,sentinels=%lu\r\n",
                primary_id++, ri->name, status,
                ri->addr->ip, ri->addr->port,
                dictSize(ri->replicas),
                dictSize(ri->sentinels)+1);
        }
        dictReleaseIterator(di);
    }

    addReplyBulkSds(c, info);
}

/* Implements Sentinel version of the ROLE command. The output is
 * "sentinel" and the list of currently monitored primary names. */
void sentinelRoleCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    addReplyMultiBulkLen(c,2);
    addReplyBulkCBuffer(c,"sentinel",8);
    addReplyMultiBulkLen(c,dictSize(sentinel.primaries));

    di = dictGetIterator(sentinel.primaries);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplyBulkCString(c,ri->name);
    }
    dictReleaseIterator(di);
}

/* SENTINEL SET <primaryname> [<option> <value> ...] */
void sentinelSetCommand(client *c) {
    sentinelRedisInstance *ri;
    int j, changes = 0;
    int badarg = 0; /* Bad argument position for error reporting. */
    char *option;

    if ((ri = sentinelGetPrimaryByNameOrReplyError(c,c->argv[2]))
        == NULL) return;

    /* Process option - value pairs. */
    for (j = 3; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        option = c->argv[j]->ptr;
        long long ll;
        int old_j = j; /* Used to know what to log as an event. */

        if (!strcasecmp(option,"down-after-milliseconds") && moreargs > 0) {
            /* down-after-millisecodns <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->down_after_period = ll;
            sentinelPropagateDownAfterPeriod(ri);
            changes++;
        } else if (!strcasecmp(option,"failover-timeout") && moreargs > 0) {
            /* failover-timeout <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->failover_timeout = ll;
            changes++;
        } else if (!strcasecmp(option,"parallel-syncs") && moreargs > 0) {
            /* parallel-syncs <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->parallel_syncs = ll;
            changes++;
        } else if (!strcasecmp(option,"notification-script") && moreargs > 0) {
            /* notification-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                    "Reconfiguration of scripts path is denied for "
                    "security reasons. Check the deny-scripts-reconfig "
                    "configuration directive in your Sentinel configuration");
                return;
            }

            if (strlen(value) && access(value,X_OK) == -1) {
                addReplyError(c,
                    "Notification script seems non existing or non executable");
                if (changes) sentinelFlushConfig();
                return;
            }
            sdsfree(ri->notification_script);
            ri->notification_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"client-reconfig-script") && moreargs > 0) {
            /* client-reconfig-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                    "Reconfiguration of scripts path is denied for "
                    "security reasons. Check the deny-scripts-reconfig "
                    "configuration directive in your Sentinel configuration");
                return;
            }

            if (strlen(value) && access(value,X_OK) == -1) {
                addReplyError(c,
                    "Client reconfiguration script seems non existing or "
                    "non executable");
                if (changes) sentinelFlushConfig();
                return;
            }
            sdsfree(ri->client_reconfig_script);
            ri->client_reconfig_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"auth-pass") && moreargs > 0) {
            /* auth-pass <password> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_pass);
            ri->auth_pass = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"quorum") && moreargs > 0) {
            /* quorum <count> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->quorum = ll;
            changes++;
        } else if (!strcasecmp(option,"rename-command") && moreargs > 1) {
            /* rename-command <oldname> <newname> */
            sds oldname = c->argv[++j]->ptr;
            sds newname = c->argv[++j]->ptr;

            if ((sdslen(oldname) == 0) || (sdslen(newname) == 0)) {
                badarg = sdslen(newname) ? j-1 : j;
                goto badfmt;
            }

            /* Remove any older renaming for this command. */
            dictDelete(ri->renamed_commands,oldname);

            /* If the target name is the same as the source name there
             * is no need to add an entry mapping to itself. */
            if (!dictSdsKeyCaseCompare(NULL,oldname,newname)) {
                oldname = sdsdup(oldname);
                newname = sdsdup(newname);
                dictAdd(ri->renamed_commands,oldname,newname);
            }
            changes++;
        } else {
            addReplyErrorFormat(c,"Unknown option or number of arguments for "
                                  "SENTINEL SET '%s'", option);
            if (changes) sentinelFlushConfig();
            return;
        }

        /* Log the event. */
        int numargs = j-old_j+1;
        switch(numargs) {
        case 2:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s %s",c->argv[old_j]->ptr,
                                                          c->argv[old_j+1]->ptr);
            break;
        case 3:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s %s %s",c->argv[old_j]->ptr,
                                                             c->argv[old_j+1]->ptr,
                                                             c->argv[old_j+2]->ptr);
            break;
        default:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s",c->argv[old_j]->ptr);
            break;
        }
    }

    if (changes) sentinelFlushConfig();
    addReply(c,shared.ok);
    return;

badfmt: /* Bad format errors */
    if (changes) sentinelFlushConfig();
    addReplyErrorFormat(c,"Invalid argument '%s' for SENTINEL SET '%s'",
        (char*)c->argv[badarg]->ptr,option);
}

/* Our fake PUBLISH command: it is actually useful only to receive hello messages
 * from the other sentinel instances, and publishing to a channel other than
 * SENTINEL_HELLO_CHANNEL is forbidden.
 *
 * Because we have a Sentinel PUBLISH, the code to send hello messages is the same
 * for all the three kind of instances: primaries, replicas, sentinels. */
void sentinelPublishCommand(client *c) {
    if (strcmp(c->argv[1]->ptr,SENTINEL_HELLO_CHANNEL)) {
        addReplyError(c, "Only HELLO messages are accepted by Sentinel instances.");
        return;
    }
    sentinelProcessHelloMessage(c->argv[2]->ptr,sdslen(c->argv[2]->ptr));
    addReplyLongLong(c,1);
}

/* ===================== SENTINEL availability checks ======================= */

/* Is this instance down from our point of view? */
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {
    mstime_t elapsed = 0;

    if (ri->link->act_ping_time)
        elapsed = mstime() - ri->link->act_ping_time;
    else if (ri->link->disconnected)
        elapsed = mstime() - ri->link->last_avail_time;

    /* Check if we are in need for a reconnection of one of the
     * links, because we are detecting low activity.
     *
     * 1) Check if the command link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have a
     *    pending ping for more than half the timeout. */
    if (ri->link->cc &&
        (mstime() - ri->link->cc_conn_time) >
        SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        ri->link->act_ping_time != 0 && /* There is a pending ping... */
        /* The pending ping is delayed, and we did not receive
         * error replies as well. */
        (mstime() - ri->link->act_ping_time) > (ri->down_after_period/2) &&
        (mstime() - ri->link->last_pong_time) > (ri->down_after_period/2))
    {
        instanceLinkCloseConnection(ri->link,ri->link->cc);
    }

    /* 2) Check if the pubsub link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have no
     *    activity in the Pub/Sub channel for more than
     *    SENTINEL_PUBLISH_PERIOD * 3.
     */
    if (ri->link->pc &&
        (mstime() - ri->link->pc_conn_time) >
         SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->link->pc_last_activity) > (SENTINEL_PUBLISH_PERIOD*3))
    {
        instanceLinkCloseConnection(ri->link,ri->link->pc);
    }

    /* Update the SDOWN flag. We believe the instance is SDOWN if:
     *
     * 1) It is not replying.
     * 2) We believe it is a primary, it reports to be a replica for enough time
     *    to meet the down_after_period, plus enough time to get two times
     *    INFO report from the instance. */
    if (elapsed > ri->down_after_period ||
        (ri->flags & SRI_PRIMARY &&
         ri->role_reported == SRI_REPLICA &&
         mstime() - ri->role_reported_time >
          (ri->down_after_period+SENTINEL_INFO_PERIOD*2)))
    {
        /* Is subjectively down */
        if ((ri->flags & SRI_S_DOWN) == 0) {
            sentinelEvent(LL_WARNING,"+sdown",ri,"%@");
            ri->s_down_since_time = mstime();
            ri->flags |= SRI_S_DOWN;
        }
    } else {
        /* Is subjectively up */
        if (ri->flags & SRI_S_DOWN) {
            sentinelEvent(LL_WARNING,"-sdown",ri,"%@");
            ri->flags &= ~(SRI_S_DOWN|SRI_SCRIPT_KILL_SENT);
        }
    }
}

/* Is this instance down according to the configured quorum?
 *
 * Note that ODOWN is a weak quorum, it only means that enough Sentinels
 * reported in a given time range that the instance was not reachable.
 * However messages can be delayed so there are no strong guarantees about
 * N instances agreeing at the same time about the down state. */
void sentinelCheckObjectivelyDown(sentinelRedisInstance *primary) {
    dictIterator *di;
    dictEntry *de;
    unsigned int quorum = 0, odown = 0;

    if (primary->flags & SRI_S_DOWN) {
        /* Is down for enough sentinels? */
        quorum = 1; /* the current sentinel. */
        /* Count all the other sentinels. */
        di = dictGetIterator(primary->sentinels);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);

            if (ri->flags & SRI_PRIMARY_DOWN) quorum++;
        }
        dictReleaseIterator(di);
        if (quorum >= primary->quorum) odown = 1;
    }

    /* Set the flag accordingly to the outcome. */
    if (odown) {
        if ((primary->flags & SRI_O_DOWN) == 0) {
            sentinelEvent(LL_WARNING,"+odown",primary,"%@ #quorum %d/%d",
                quorum, primary->quorum);
            primary->flags |= SRI_O_DOWN;
            primary->o_down_since_time = mstime();
        }
    } else {
        if (primary->flags & SRI_O_DOWN) {
            sentinelEvent(LL_WARNING,"-odown",primary,"%@");
            primary->flags &= ~SRI_O_DOWN;
        }
    }
}

/* Receive the SENTINEL is-primary-down-by-addr reply, see the
 * sentinelAskPrimaryStateToOtherSentinels() function for more information. */
void sentinelReceiveIsPrimaryDownReply(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Ignore every error or unexpected reply.
     * Note that if the command returns an error for any reason we'll
     * end clearing the SRI_PRIMARY_DOWN flag for timeout anyway. */
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 3 &&
        r->element[0]->type == REDIS_REPLY_INTEGER &&
        r->element[1]->type == REDIS_REPLY_STRING &&
        r->element[2]->type == REDIS_REPLY_INTEGER)
    {
        ri->last_primary_down_reply_time = mstime();
        if (r->element[0]->integer == 1) {
            ri->flags |= SRI_PRIMARY_DOWN;
        } else {
            ri->flags &= ~SRI_PRIMARY_DOWN;
        }
        if (strcmp(r->element[1]->str,"*")) {
            /* If the runid in the reply is not "*" the Sentinel actually
             * replied with a vote. */
            sdsfree(ri->leader);
            if ((long long)ri->leader_epoch != r->element[2]->integer)
                serverLog(LL_WARNING,
                    "%s voted for %s %llu", ri->name,
                    r->element[1]->str,
                    (unsigned long long) r->element[2]->integer);
            ri->leader = sdsnew(r->element[1]->str);
            ri->leader_epoch = r->element[2]->integer;
        }
    }
}

/* If we think the primary is down, we start sending
 * SENTINEL IS-PRIMARY-DOWN-BY-ADDR requests to other sentinels
 * in order to get the replies that allow to reach the quorum
 * needed to mark the primary in ODOWN state and trigger a failover. */
#define SENTINEL_ASK_FORCED (1<<0)
void sentinelAskPrimaryStateToOtherSentinels(sentinelRedisInstance *primary, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(primary->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        mstime_t elapsed = mstime() - ri->last_primary_down_reply_time;
        char port[32];
        int retval;

        /* If the primary state from other sentinel is too old, we clear it. */
        if (elapsed > SENTINEL_ASK_PERIOD*5) {
            ri->flags &= ~SRI_PRIMARY_DOWN;
            sdsfree(ri->leader);
            ri->leader = NULL;
        }

        /* Only ask if primary is down to other sentinels if:
         *
         * 1) We believe it is down, or there is a failover in progress.
         * 2) Sentinel is connected.
         * 3) We did not receive the info within SENTINEL_ASK_PERIOD ms. */
        if ((primary->flags & SRI_S_DOWN) == 0) continue;
        if (ri->link->disconnected) continue;
        if (!(flags & SENTINEL_ASK_FORCED) &&
            mstime() - ri->last_primary_down_reply_time < SENTINEL_ASK_PERIOD)
            continue;

        /* Ask */
        ll2string(port,sizeof(port),primary->addr->port);
        retval = redisAsyncCommand(ri->link->cc,
                    sentinelReceiveIsPrimaryDownReply, ri,
                    "%s is-primary-down-by-addr %s %s %llu %s",
                    sentinelInstanceMapCommand(ri,"SENTINEL"),
                    primary->addr->ip, port,
                    sentinel.current_epoch,
                    (primary->failover_state > SENTINEL_FAILOVER_STATE_NONE) ?
                    sentinel.myid : "*");
        if (retval == C_OK) ri->link->pending_commands++;
    }
    dictReleaseIterator(di);
}

/* =============================== FAILOVER ================================= */

/* Crash because of user request via SENTINEL simulate-failure command. */
void sentinelSimFailureCrash(void) {
    serverLog(LL_WARNING,
        "Sentinel CRASH because of SENTINEL simulate-failure");
    exit(99);
}

/* Vote for the sentinel with 'req_runid' or return the old vote if already
 * voted for the specified 'req_epoch' or one greater.
 *
 * If a vote is not available returns NULL, otherwise return the Sentinel
 * runid and populate the leader_epoch with the epoch of the vote. */
char *sentinelVoteLeader(sentinelRedisInstance *primary, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch) {
    if (req_epoch > sentinel.current_epoch) {
        sentinel.current_epoch = req_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING,"+new-epoch",primary,"%llu",
            (unsigned long long) sentinel.current_epoch);
    }

    if (primary->leader_epoch < req_epoch && sentinel.current_epoch <= req_epoch)
    {
        sdsfree(primary->leader);
        primary->leader = sdsnew(req_runid);
        primary->leader_epoch = sentinel.current_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING,"+vote-for-leader",primary,"%s %llu",
            primary->leader, (unsigned long long) primary->leader_epoch);
        /* If we did not voted for ourselves, set the primary failover start
         * time to now, in order to force a delay before we can start a
         * failover for the same primary. */
        if (strcasecmp(primary->leader,sentinel.myid))
            primary->failover_start_time = mstime()+rand()%SENTINEL_MAX_DESYNC;
    }

    *leader_epoch = primary->leader_epoch;
    return primary->leader ? sdsnew(primary->leader) : NULL;
}

struct sentinelLeader {
    char *runid;
    unsigned long votes;
};

/* Helper function for sentinelGetLeader, increment the counter
 * relative to the specified runid. */
int sentinelLeaderIncr(dict *counters, char *runid) {
    dictEntry *existing, *de;
    uint64_t oldval;

    de = dictAddRaw(counters,runid,&existing);
    if (existing) {
        oldval = dictGetUnsignedIntegerVal(existing);
        dictSetUnsignedIntegerVal(existing,oldval+1);
        return oldval+1;
    } else {
        serverAssert(de != NULL);
        dictSetUnsignedIntegerVal(de,1);
        return 1;
    }
}

/* Scan all the Sentinels attached to this primary to check if there
 * is a leader for the specified epoch.
 *
 * To be a leader for a given epoch, we should have the majority of
 * the Sentinels we know (ever seen since the last SENTINEL RESET) that
 * reported the same instance as leader for the same epoch. */
char *sentinelGetLeader(sentinelRedisInstance *primary, uint64_t epoch) {
    dict *counters;
    dictIterator *di;
    dictEntry *de;
    unsigned int voters = 0, voters_quorum;
    char *myvote;
    char *winner = NULL;
    uint64_t leader_epoch;
    uint64_t max_votes = 0;

    serverAssert(primary->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS));
    counters = dictCreate(&leaderVotesDictType,NULL);

    voters = dictSize(primary->sentinels)+1; /* All the other sentinels and me.*/

    /* Count other sentinels votes */
    di = dictGetIterator(primary->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->leader != NULL && ri->leader_epoch == sentinel.current_epoch)
            sentinelLeaderIncr(counters,ri->leader);
    }
    dictReleaseIterator(di);

    /* Check what's the winner. For the winner to win, it needs two conditions:
     * 1) Absolute majority between voters (50% + 1).
     * 2) And anyway at least primary->quorum votes. */
    di = dictGetIterator(counters);
    while((de = dictNext(di)) != NULL) {
        uint64_t votes = dictGetUnsignedIntegerVal(de);

        if (votes > max_votes) {
            max_votes = votes;
            winner = dictGetKey(de);
        }
    }
    dictReleaseIterator(di);

    /* Count this Sentinel vote:
     * if this Sentinel did not voted yet, either vote for the most
     * common voted sentinel, or for itself if no vote exists at all. */
    if (winner)
        myvote = sentinelVoteLeader(primary,epoch,winner,&leader_epoch);
    else
        myvote = sentinelVoteLeader(primary,epoch,sentinel.myid,&leader_epoch);

    if (myvote && leader_epoch == epoch) {
        uint64_t votes = sentinelLeaderIncr(counters,myvote);

        if (votes > max_votes) {
            max_votes = votes;
            winner = myvote;
        }
    }

    voters_quorum = voters/2+1;
    if (winner && (max_votes < voters_quorum || max_votes < primary->quorum))
        winner = NULL;

    winner = winner ? sdsnew(winner) : NULL;
    sdsfree(myvote);
    dictRelease(counters);
    return winner;
}

/* Send REPLICAOF to the specified instance, always followed by a
 * CONFIG REWRITE command in order to store the new configuration on disk
 * when possible (that is, if the Redis instance is recent enough to support
 * config rewriting, and if the server was started with a configuration file).
 *
 * If Host is NULL the function sends "REPLICAOF NO ONE".
 *
 * The command returns C_OK if the REPLICAOF command was accepted for
 * (later) delivery otherwise C_ERR. The command replies are just
 * discarded. */
int sentinelSendReplicaOf(sentinelRedisInstance *ri, char *host, int port) {
    char portstr[32];
    int retval;

    ll2string(portstr,sizeof(portstr),port);

    /* If host is NULL we send REPLICAOF NO ONE that will turn the instance
     * into a primary. */
    if (host == NULL) {
        host = "NO";
        memcpy(portstr,"ONE",4);
    }

    /* In order to send REPLICAOF in a safe way, we send a transaction performing
     * the following tasks:
     * 1) Reconfigure the instance according to the specified host/port params.
     * 2) Rewrite the configuration.
     * 3) Disconnect all clients (but this one sending the commnad) in order
     *    to trigger the ask-primary-on-reconnection protocol for connected
     *    clients.
     *
     * Note that we don't check the replies returned by commands, since we
     * will observe instead the effects in the next INFO output. */
    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"MULTI"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s %s %s",
        sentinelInstanceMapCommand(ri,"REPLICAOF"),
        host, portstr);
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s REWRITE",
        sentinelInstanceMapCommand(ri,"CONFIG"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    /* CLIENT KILL TYPE <type> is only supported starting from Redis 2.8.12,
     * however sending it to an instance not understanding this command is not
     * an issue because CLIENT is variadic command, so Redis will not
     * recognized as a syntax error, and the transaction will not fail (but
     * only the unsupported command will fail). */
    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s KILL TYPE normal",
        sentinelInstanceMapCommand(ri,"CLIENT"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"EXEC"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    return C_OK;
}

/* Setup the primary state to start a failover. */
void sentinelStartFailover(sentinelRedisInstance *primary) {
    serverAssert(primary->flags & SRI_PRIMARY);

    primary->failover_state = SENTINEL_FAILOVER_STATE_WAIT_START;
    primary->flags |= SRI_FAILOVER_IN_PROGRESS;
    primary->failover_epoch = ++sentinel.current_epoch;
    sentinelEvent(LL_WARNING,"+new-epoch",primary,"%llu",
        (unsigned long long) sentinel.current_epoch);
    sentinelEvent(LL_WARNING,"+try-failover",primary,"%@");
    primary->failover_start_time = mstime()+rand()%SENTINEL_MAX_DESYNC;
    primary->failover_state_change_time = mstime();
}

/* This function checks if there are the conditions to start the failover,
 * that is:
 *
 * 1) Primary must be in ODOWN condition.
 * 2) No failover already in progress.
 * 3) No failover already attempted recently.
 *
 * We still don't know if we'll win the election so it is possible that we
 * start the failover but that we'll not be able to act.
 *
 * Return non-zero if a failover was started. */
int sentinelStartFailoverIfNeeded(sentinelRedisInstance *primary) {
    /* We can't failover if the primary is not in O_DOWN state. */
    if (!(primary->flags & SRI_O_DOWN)) return 0;

    /* Failover already in progress? */
    if (primary->flags & SRI_FAILOVER_IN_PROGRESS) return 0;

    /* Last failover attempt started too little time ago? */
    if (mstime() - primary->failover_start_time <
        primary->failover_timeout*2)
    {
        if (primary->failover_delay_logged != primary->failover_start_time) {
            time_t clock = (primary->failover_start_time +
                            primary->failover_timeout*2) / 1000;
            char ctimebuf[26];

            ctime_r(&clock,ctimebuf);
            ctimebuf[24] = '\0'; /* Remove newline. */
            primary->failover_delay_logged = primary->failover_start_time;
            serverLog(LL_WARNING,
                "Next failover delay: I will not start a failover before %s",
                ctimebuf);
        }
        return 0;
    }

    sentinelStartFailover(primary);
    return 1;
}

/* Select a suitable replica to promote. The current algorithm only uses
 * the following parameters:
 *
 * 1) None of the following conditions: S_DOWN, O_DOWN, DISCONNECTED.
 * 2) Last time the replica replied to ping no more than 5 times the PING period.
 * 3) info_refresh not older than 3 times the INFO refresh period.
 * 4) primary_link_down_time no more than:
 *     (now - primary->s_down_since_time) + (primary->down_after_period * 10).
 *    Basically since the primary is down from our POV, the replica reports
 *    to be disconnected no more than 10 times the configured down-after-period.
 *    This is pretty much black magic but the idea is, the primary was not
 *    available so the replica may be lagging, but not over a certain time.
 *    Anyway we'll select the best replica according to replication offset.
 * 5) Replica priority can't be zero, otherwise the replica is discarded.
 *
 * Among all the replicas matching the above conditions we select the replica
 * with, in order of sorting key:
 *
 * - lower replica_priority.
 * - bigger processed replication offset.
 * - lexicographically smaller runid.
 *
 * Basically if runid is the same, the replica that processed more commands
 * from the primary is selected.
 *
 * The function returns the pointer to the selected replica, otherwise
 * NULL if no suitable replica was found.
 */

/* Helper for sentinelSelectReplica(). This is used by qsort() in order to
 * sort suitable replicas in a "better first" order, to take the first of
 * the list. */
int compareReplicasForPromotion(const void *a, const void *b) {
    sentinelRedisInstance **sa = (sentinelRedisInstance **)a,
                          **sb = (sentinelRedisInstance **)b;
    char *sa_runid, *sb_runid;

    if ((*sa)->replica_priority != (*sb)->replica_priority)
        return (*sa)->replica_priority - (*sb)->replica_priority;

    /* If priority is the same, select the replica with greater replication
     * offset (processed more data from the primary). */
    if ((*sa)->replica_repl_offset > (*sb)->replica_repl_offset) {
        return -1; /* a < b */
    } else if ((*sa)->replica_repl_offset < (*sb)->replica_repl_offset) {
        return 1; /* a > b */
    }

    /* If the replication offset is the same select the replica with that has
     * the lexicographically smaller runid. Note that we try to handle runid
     * == NULL as there are old Redis versions that don't publish runid in
     * INFO. A NULL runid is considered bigger than any other runid. */
    sa_runid = (*sa)->runid;
    sb_runid = (*sb)->runid;
    if (sa_runid == NULL && sb_runid == NULL) return 0;
    else if (sa_runid == NULL) return 1;  /* a > b */
    else if (sb_runid == NULL) return -1; /* a < b */
    return strcasecmp(sa_runid, sb_runid);
}

sentinelRedisInstance *sentinelSelectReplica(sentinelRedisInstance *primary) {
    sentinelRedisInstance **instance =
        zmalloc(sizeof(instance[0])*dictSize(primary->replicas));
    sentinelRedisInstance *selected = NULL;
    int instances = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t max_primary_down_time = 0;

    if (primary->flags & SRI_S_DOWN)
        max_primary_down_time += mstime() - primary->s_down_since_time;
    max_primary_down_time += primary->down_after_period * 10;

    di = dictGetIterator(primary->replicas);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *replica = dictGetVal(de);
        mstime_t info_validity_time;

        if (replica->flags & (SRI_S_DOWN|SRI_O_DOWN)) continue;
        if (replica->link->disconnected) continue;
        if (mstime() - replica->link->last_avail_time > SENTINEL_PING_PERIOD*5) continue;
        if (replica->replica_priority == 0) continue;

        /* If the primary is in SDOWN state we get INFO for replicas every second.
         * Otherwise we get it with the usual period so we need to account for
         * a larger delay. */
        if (primary->flags & SRI_S_DOWN)
            info_validity_time = SENTINEL_PING_PERIOD*5;
        else
            info_validity_time = SENTINEL_INFO_PERIOD*3;
        if (mstime() - replica->info_refresh > info_validity_time) continue;
        if (replica->primary_link_down_time > max_primary_down_time) continue;
        instance[instances++] = replica;
    }
    dictReleaseIterator(di);
    if (instances) {
        qsort(instance,instances,sizeof(sentinelRedisInstance*),
            compareReplicasForPromotion);
        selected = instance[0];
    }
    zfree(instance);
    return selected;
}

/* ---------------- Failover state machine implementation ------------------- */
void sentinelFailoverWaitStart(sentinelRedisInstance *ri) {
    char *leader;
    int isleader;

    /* Check if we are the leader for the failover epoch. */
    leader = sentinelGetLeader(ri, ri->failover_epoch);
    isleader = leader && strcasecmp(leader,sentinel.myid) == 0;
    sdsfree(leader);

    /* If I'm not the leader, and it is not a forced failover via
     * SENTINEL FAILOVER, then I can't continue with the failover. */
    if (!isleader && !(ri->flags & SRI_FORCE_FAILOVER)) {
        int election_timeout = SENTINEL_ELECTION_TIMEOUT;

        /* The election timeout is the MIN between SENTINEL_ELECTION_TIMEOUT
         * and the configured failover timeout. */
        if (election_timeout > ri->failover_timeout)
            election_timeout = ri->failover_timeout;
        /* Abort the failover if I'm not the leader after some time. */
        if (mstime() - ri->failover_start_time > election_timeout) {
            sentinelEvent(LL_WARNING,"-failover-abort-not-elected",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }
    sentinelEvent(LL_WARNING,"+elected-leader",ri,"%@");
    if (sentinel.simfailure_flags & SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION)
        sentinelSimFailureCrash();
    ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_REPLICA;
    ri->failover_state_change_time = mstime();
    sentinelEvent(LL_WARNING,"+failover-state-select-replica",ri,"%@");
}

void sentinelFailoverSelectReplica(sentinelRedisInstance *ri) {
    sentinelRedisInstance *replica = sentinelSelectReplica(ri);

    /* We don't handle the timeout in this state as the function aborts
     * the failover or go forward in the next state. */
    if (replica == NULL) {
        sentinelEvent(LL_WARNING,"-failover-abort-no-good-replica",ri,"%@");
        sentinelAbortFailover(ri);
    } else {
        sentinelEvent(LL_WARNING,"+selected-replica",replica,"%@");
        replica->flags |= SRI_PROMOTED;
        ri->promoted_replica = replica;
        ri->failover_state = SENTINEL_FAILOVER_STATE_SEND_REPLICAOF_NOONE;
        ri->failover_state_change_time = mstime();
        sentinelEvent(LL_NOTICE,"+failover-state-send-replicaof-noone",
            replica, "%@");
    }
}

void sentinelFailoverSendReplicaOfNoOne(sentinelRedisInstance *ri) {
    int retval;

    /* We can't send the command to the promoted replica if it is now
     * disconnected. Retry again and again with this state until the timeout
     * is reached, then abort the failover. */
    if (ri->promoted_replica->link->disconnected) {
        if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
            sentinelEvent(LL_WARNING,"-failover-abort-replica-timeout",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    /* Send REPLICAOF NO ONE command to turn the replica into a primary.
     * We actually register a generic callback for this command as we don't
     * really care about the reply. We check if it worked indirectly observing
     * if INFO returns a different role (primary instead of replica). */
    retval = sentinelSendReplicaOf(ri->promoted_replica,NULL,0);
    if (retval != C_OK) return;
    sentinelEvent(LL_NOTICE, "+failover-state-wait-promotion",
        ri->promoted_replica,"%@");
    ri->failover_state = SENTINEL_FAILOVER_STATE_WAIT_PROMOTION;
    ri->failover_state_change_time = mstime();
}

/* We actually wait for promotion indirectly checking with INFO when the
 * replica turns into a primary. */
void sentinelFailoverWaitPromotion(sentinelRedisInstance *ri) {
    /* Just handle the timeout. Switching to the next state is handled
     * by the function parsing the INFO command of the promoted replica. */
    if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
        sentinelEvent(LL_WARNING,"-failover-abort-replica-timeout",ri,"%@");
        sentinelAbortFailover(ri);
    }
}

void sentinelFailoverDetectEnd(sentinelRedisInstance *primary) {
    int not_reconfigured = 0, timeout = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t elapsed = mstime() - primary->failover_state_change_time;

    /* We can't consider failover finished if the promoted replica is
     * not reachable. */
    if (primary->promoted_replica == NULL ||
        primary->promoted_replica->flags & SRI_S_DOWN) return;

    /* The failover terminates once all the reachable replicas are properly
     * configured. */
    di = dictGetIterator(primary->replicas);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *replica = dictGetVal(de);

        if (replica->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;
        if (replica->flags & SRI_S_DOWN) continue;
        not_reconfigured++;
    }
    dictReleaseIterator(di);

    /* Force end of failover on timeout. */
    if (elapsed > primary->failover_timeout) {
        not_reconfigured = 0;
        timeout = 1;
        sentinelEvent(LL_WARNING,"+failover-end-for-timeout",primary,"%@");
    }

    if (not_reconfigured == 0) {
        sentinelEvent(LL_WARNING,"+failover-end",primary,"%@");
        primary->failover_state = SENTINEL_FAILOVER_STATE_UPDATE_CONFIG;
        primary->failover_state_change_time = mstime();
    }

    /* If I'm the leader it is a good idea to send a best effort REPLICAOF
     * command to all the replicas still not reconfigured to replicate with
     * the new primary. */
    if (timeout) {
        dictIterator *di;
        dictEntry *de;

        di = dictGetIterator(primary->replicas);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *replica = dictGetVal(de);
            int retval;

            if (replica->flags & (SRI_RECONF_DONE|SRI_RECONF_SENT)) continue;
            if (replica->link->disconnected) continue;

            retval = sentinelSendReplicaOf(replica,
                    primary->promoted_replica->addr->ip,
                    primary->promoted_replica->addr->port);
            if (retval == C_OK) {
                sentinelEvent(LL_NOTICE,"+replica-reconf-sent-be",replica,"%@");
                replica->flags |= SRI_RECONF_SENT;
            }
        }
        dictReleaseIterator(di);
    }
}

/* Send REPLICA OF <new primary address> to all the remaining replicas that
 * still don't appear to have the configuration updated. */
void sentinelFailoverReconfNextReplica(sentinelRedisInstance *primary) {
    dictIterator *di;
    dictEntry *de;
    int in_progress = 0;

    di = dictGetIterator(primary->replicas);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *replica = dictGetVal(de);

        if (replica->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG))
            in_progress++;
    }
    dictReleaseIterator(di);

    di = dictGetIterator(primary->replicas);
    while(in_progress < primary->parallel_syncs &&
          (de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *replica = dictGetVal(de);
        int retval;

        /* Skip the promoted replica, and already configured replicas. */
        if (replica->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;

        /* If too much time elapsed without the replica moving forward to
         * the next state, consider it reconfigured even if it is not.
         * Sentinels will detect the replica as misconfigured and fix its
         * configuration later. */
        if ((replica->flags & SRI_RECONF_SENT) &&
            (mstime() - replica->replica_reconf_sent_time) >
            SENTINEL_REPLICA_RECONF_TIMEOUT)
        {
            sentinelEvent(LL_NOTICE,"-replica-reconf-sent-timeout",replica,"%@");
            replica->flags &= ~SRI_RECONF_SENT;
            replica->flags |= SRI_RECONF_DONE;
        }

        /* Nothing to do for instances that are disconnected or already
         * in RECONF_SENT state. */
        if (replica->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)) continue;
        if (replica->link->disconnected) continue;

        /* Send REPLICAOF <new primary>. */
        retval = sentinelSendReplicaOf(replica,
                primary->promoted_replica->addr->ip,
                primary->promoted_replica->addr->port);
        if (retval == C_OK) {
            replica->flags |= SRI_RECONF_SENT;
            replica->replica_reconf_sent_time = mstime();
            sentinelEvent(LL_NOTICE,"+replica-reconf-sent",replica,"%@");
            in_progress++;
        }
    }
    dictReleaseIterator(di);

    /* Check if all the replicas are reconfigured and handle timeout. */
    sentinelFailoverDetectEnd(primary);
}

/* This function is called when the replica is in
 * SENTINEL_FAILOVER_STATE_UPDATE_CONFIG state. In this state we need
 * to remove it from the primary table and add the promoted replica instead. */
void sentinelFailoverSwitchToPromotedReplica(sentinelRedisInstance *primary) {
    sentinelRedisInstance *ref = primary->promoted_replica ?
                                 primary->promoted_replica : primary;

    sentinelEvent(LL_WARNING,"+switch-primary",primary,"%s %s %d %s %d",
        primary->name, primary->addr->ip, primary->addr->port,
        ref->addr->ip, ref->addr->port);

    sentinelResetPrimaryAndChangeAddress(primary,ref->addr->ip,ref->addr->port);
}

void sentinelFailoverStateMachine(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_PRIMARY);

    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS)) return;

    switch(ri->failover_state) {
        case SENTINEL_FAILOVER_STATE_WAIT_START:
            sentinelFailoverWaitStart(ri);
            break;
        case SENTINEL_FAILOVER_STATE_SELECT_REPLICA:
            sentinelFailoverSelectReplica(ri);
            break;
        case SENTINEL_FAILOVER_STATE_SEND_REPLICAOF_NOONE:
            sentinelFailoverSendReplicaOfNoOne(ri);
            break;
        case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
            sentinelFailoverWaitPromotion(ri);
            break;
        case SENTINEL_FAILOVER_STATE_RECONF_REPLICAS:
            sentinelFailoverReconfNextReplica(ri);
            break;
    }
}

/* Abort a failover in progress:
 *
 * This function can only be called before the promoted replica acknowledged
 * the replica -> primary switch. Otherwise the failover can't be aborted and
 * will reach its end (possibly by timeout). */
void sentinelAbortFailover(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_FAILOVER_IN_PROGRESS);
    serverAssert(ri->failover_state <= SENTINEL_FAILOVER_STATE_WAIT_PROMOTION);

    ri->flags &= ~(SRI_FAILOVER_IN_PROGRESS|SRI_FORCE_FAILOVER);
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = mstime();
    if (ri->promoted_replica) {
        ri->promoted_replica->flags &= ~SRI_PROMOTED;
        ri->promoted_replica = NULL;
    }
}

/* ======================== SENTINEL timer handler ==========================
 * This is the "main" our Sentinel, being sentinel completely non blocking
 * in design. The function is called every second.
 * -------------------------------------------------------------------------- */

/* Perform scheduled operations for the specified Redis instance. */
void sentinelHandleRedisInstance(sentinelRedisInstance *ri) {
    /* ========== MONITORING HALF ============ */
    /* Every kind of instance */
    sentinelReconnectInstance(ri);
    sentinelSendPeriodicCommands(ri);

    /* ============== ACTING HALF ============= */
    /* We don't proceed with the acting half if we are in TILT mode.
     * TILT happens when we find something odd with the time, like a
     * sudden change in the clock. */
    if (sentinel.tilt) {
        if (mstime()-sentinel.tilt_start_time < SENTINEL_TILT_PERIOD) return;
        sentinel.tilt = 0;
        sentinelEvent(LL_WARNING,"-tilt",NULL,"#tilt mode exited");
    }

    /* Every kind of instance */
    sentinelCheckSubjectivelyDown(ri);

    /* Primaries and replicas */
    if (ri->flags & (SRI_PRIMARY|SRI_REPLICA)) {
        /* Nothing so far. */
    }

    /* Only primaries */
    if (ri->flags & SRI_PRIMARY) {
        sentinelCheckObjectivelyDown(ri);
        if (sentinelStartFailoverIfNeeded(ri))
            sentinelAskPrimaryStateToOtherSentinels(ri,SENTINEL_ASK_FORCED);
        sentinelFailoverStateMachine(ri);
        sentinelAskPrimaryStateToOtherSentinels(ri,SENTINEL_NO_FLAGS);
    }
}

/* Perform scheduled operations for all the instances in the dictionary.
 * Recursively call the function against dictionaries of replicas. */
void sentinelHandleDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *switch_to_promoted = NULL;

    /* There are a number of things we need to perform against every primary. */
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        sentinelHandleRedisInstance(ri);
        if (ri->flags & SRI_PRIMARY) {
            sentinelHandleDictOfRedisInstances(ri->replicas);
            sentinelHandleDictOfRedisInstances(ri->sentinels);
            if (ri->failover_state == SENTINEL_FAILOVER_STATE_UPDATE_CONFIG) {
                switch_to_promoted = ri;
            }
        }
    }
    if (switch_to_promoted)
        sentinelFailoverSwitchToPromotedReplica(switch_to_promoted);
    dictReleaseIterator(di);
}

/* This function checks if we need to enter the TITL mode.
 *
 * The TILT mode is entered if we detect that between two invocations of the
 * timer interrupt, a negative amount of time, or too much time has passed.
 * Note that we expect that more or less just 100 milliseconds will pass
 * if everything is fine. However we'll see a negative number or a
 * difference bigger than SENTINEL_TILT_TRIGGER milliseconds if one of the
 * following conditions happen:
 *
 * 1) The Sentiel process for some time is blocked, for every kind of
 * random reason: the load is huge, the computer was frozen for some time
 * in I/O or alike, the process was stopped by a signal. Everything.
 * 2) The system clock was altered significantly.
 *
 * Under both this conditions we'll see everything as timed out and failing
 * without good reasons. Instead we enter the TILT mode and wait
 * for SENTINEL_TILT_PERIOD to elapse before starting to act again.
 *
 * During TILT time we still collect information, we just do not act. */
void sentinelCheckTiltCondition(void) {
    mstime_t now = mstime();
    mstime_t delta = now - sentinel.previous_time;

    if (delta < 0 || delta > SENTINEL_TILT_TRIGGER) {
        sentinel.tilt = 1;
        sentinel.tilt_start_time = mstime();
        sentinelEvent(LL_WARNING,"+tilt",NULL,"#tilt mode entered");
    }
    sentinel.previous_time = mstime();
}

void sentinelTimer(void) {
    sentinelCheckTiltCondition();
    sentinelHandleDictOfRedisInstances(sentinel.primaries);
    sentinelRunPendingScripts();
    sentinelCollectTerminatedScripts();
    sentinelKillTimedoutScripts();

    /* We continuously change the frequency of the Redis "timer interrupt"
     * in order to desynchronize every Sentinel from every other.
     * This non-determinism avoids that Sentinels started at the same time
     * exactly continue to stay synchronized asking to be voted at the
     * same time again and again (resulting in nobody likely winning the
     * election because of split brain voting). */
    server.hz = CONFIG_DEFAULT_HZ + rand() % CONFIG_DEFAULT_HZ;
}

