/* Asynchronous replication implementation.
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

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

void replicationDiscardCachedPrimary(void);
void replicationResurrectCachedPrimary(int newfd);
void replicationSendAck(void);
void putReplicaOnline(client *replica);
int cancelReplicationHandshake(void);

/* --------------------------- Utility functions ---------------------------- */

/* Return the pointer to a string representing the replica ip:listening_port
 * pair. Mostly useful for logging, since we want to log a replica using its
 * IP address and its listening port which is more clear for the user, for
 * example: "Closing connection with replica 10.1.2.3:6380". */
char *replicationGetReplicaName(client *c) {
    static char buf[NET_PEER_ID_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';
    if (c->replica_ip[0] != '\0' ||
        anetPeerToString(c->fd,ip,sizeof(ip),NULL) != -1)
    {
        /* Note that the 'ip' buffer is always larger than 'c->replica_ip' */
        if (c->replica_ip[0] != '\0') memcpy(ip,c->replica_ip,sizeof(c->replica_ip));

        if (c->replica_listening_port)
            anetFormatAddr(buf,sizeof(buf),ip,c->replica_listening_port);
        else
            snprintf(buf,sizeof(buf),"%s:<unknown-replica-port>",ip);
    } else {
        snprintf(buf,sizeof(buf),"client id #%llu",
            (unsigned long long) c->id);
    }
    return buf;
}

/* ---------------------------------- PRIMARY -------------------------------- */

void createReplicationBacklog(void) {
    serverAssert(server.repl_backlog == NULL);
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;

    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream. */
    server.repl_backlog_off = server.primary_repl_offset+1;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to both update the
 * server.repl_backlog_size and to resize the buffer and setup it so that
 * it contains the same data as the previous one (possibly less data, but
 * the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
void resizeReplicationBacklog(long long newsize) {
    if (newsize < CONFIG_REPL_BACKLOG_MIN_SIZE)
        newsize = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog_size == newsize) return;

    server.repl_backlog_size = newsize;
    if (server.repl_backlog != NULL) {
        /* What we actually do is to flush the old buffer and realloc a new
         * empty one. It will refill with new data incrementally.
         * The reason is that copying a few gigabytes adds latency and even
         * worse often we need to alloc additional space before freeing the
         * old buffer. */
        zfree(server.repl_backlog);
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        /* Next byte we have is... the next since the buffer is empty. */
        server.repl_backlog_off = server.primary_repl_offset+1;
    }
}

void freeReplicationBacklog(void) {
    serverAssert(listLength(server.replicas) == 0);
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/* Add data to the replication backlog.
 * This function also increments the global replication offset stored at
 * server.primary_repl_offset, because there is no case where we want to feed
 * the backlog without incrementing the offset. */
void feedReplicationBacklog(void *ptr, size_t len) {
    unsigned char *p = ptr;

    server.primary_repl_offset += len;

    /* This is a circular buffer, so write as much data we can at every
     * iteration and rewind the "idx" index if we reach the limit. */
    while(len) {
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;
        if (thislen > len) thislen = len;
        memcpy(server.repl_backlog+server.repl_backlog_idx,p,thislen);
        server.repl_backlog_idx += thislen;
        if (server.repl_backlog_idx == server.repl_backlog_size)
            server.repl_backlog_idx = 0;
        len -= thislen;
        p += thislen;
        server.repl_backlog_histlen += thislen;
    }
    if (server.repl_backlog_histlen > server.repl_backlog_size)
        server.repl_backlog_histlen = server.repl_backlog_size;
    /* Set the offset of the first byte we have in the backlog. */
    server.repl_backlog_off = server.primary_repl_offset -
                              server.repl_backlog_histlen + 1;
}

/* Wrapper for feedReplicationBacklog() that takes Redis string objects
 * as input. */
void feedReplicationBacklogWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBacklog(p,len);
}

/* Propagate write commands to replicas, and populate the replication backlog
 * as well. This function is used if the instance is a primary: we use
 * the commands received by our clients in order to create the replication
 * stream. Instead if the instance is a replica and has sub-replicas attached,
 * we use replicationFeedReplicasFromPrimary() */
void replicationFeedReplicas(list *replicas, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j, len;
    char llstr[LONG_STR_SIZE];

    /* If the instance is not a top level primary, return ASAP: we'll just proxy
     * the stream of data we receive from our primary instead, in order to
     * propagate *identical* replication stream. In this way this replica can
     * advertise the same replication ID as the primary (since it shares the
     * primary replication history and has the same backlog and offsets). */
    if (server.primaryhost != NULL) return;

    /* If there aren't replicas, and there is no backlog buffer to populate,
     * we can return ASAP. */
    if (server.repl_backlog == NULL && listLength(replicas) == 0) return;

    /* We can't have replicas attached and no backlog. */
    serverAssert(!(listLength(replicas) != 0 && server.repl_backlog == NULL));

    /* Send SELECT command to every replica if needed. */
    if (server.replicaseldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        /* Add the SELECT command into the backlog. */
        if (server.repl_backlog) feedReplicationBacklogWithObject(selectcmd);

        /* Send it to replicas. */
        listRewind(replicas,&li);
        while((ln = listNext(&li))) {
            client *replica = ln->value;
            if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_START) continue;
            addReply(replica,selectcmd);
        }

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    server.replicaseldb = dictid;

    /* Write the command to the replication backlog if any. */
    if (server.repl_backlog) {
        char aux[LONG_STR_SIZE+3];

        /* Add the multi bulk reply length. */
        aux[0] = '*';
        len = ll2string(aux+1,sizeof(aux)-1,argc);
        aux[len+1] = '\r';
        aux[len+2] = '\n';
        feedReplicationBacklog(aux,len+3);

        for (j = 0; j < argc; j++) {
            long objlen = stringObjectLen(argv[j]);

            /* We need to feed the buffer with the object as a bulk reply
             * not just as a plain string, so create the $..CRLF payload len
             * and add the final CRLF */
            aux[0] = '$';
            len = ll2string(aux+1,sizeof(aux)-1,objlen);
            aux[len+1] = '\r';
            aux[len+2] = '\n';
            feedReplicationBacklog(aux,len+3);
            feedReplicationBacklogWithObject(argv[j]);
            feedReplicationBacklog(aux+len+1,2);
        }
    }

    /* Write the command to every replica. */
    listRewind(replicas,&li);
    while((ln = listNext(&li))) {
        client *replica = ln->value;

        /* Don't feed replicas that are still waiting for BGSAVE to start */
        if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_START) continue;

        /* Feed replicas that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the initial SYNC completes),
         * or are already in sync with the primary. */

        /* Add the multi bulk length. */
        addReplyMultiBulkLen(replica,argc);

        /* Finally any additional argument that was not stored inside the
         * static buffer if any (from j to argc). */
        for (j = 0; j < argc; j++)
            addReplyBulk(replica,argv[j]);
    }
}

/* This function is used in order to proxy what we receive from our primary
 * to our sub-replicas. */
#include <ctype.h>
void replicationFeedReplicasFromPrimaryStream(list *replicas, char *buf, size_t buflen) {
    listNode *ln;
    listIter li;

    /* Debugging: this is handy to see the stream sent from primary
     * to replicas. Disabled with if(0). */
    if (0) {
        printf("%zu:",buflen);
        for (size_t j = 0; j < buflen; j++) {
            printf("%c", isprint(buf[j]) ? buf[j] : '.');
        }
        printf("\n");
    }

    if (server.repl_backlog) feedReplicationBacklog(buf,buflen);
    listRewind(replicas,&li);
    while((ln = listNext(&li))) {
        client *replica = ln->value;

        /* Don't feed replicas that are still waiting for BGSAVE to start */
        if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_START) continue;
        addReplyString(replica,buf,buflen);
    }
}

void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & CLIENT_LUA) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(OBJ_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

/* Feed the replica 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
long long addReplyReplicationBacklog(client *c, long long offset) {
    long long j, skip, len;

    serverLog(LL_DEBUG, "[PSYNC] Replica request offset: %lld", offset);

    if (server.repl_backlog_histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld",
             server.repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld",
             server.repl_backlog_off);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld",
             server.repl_backlog_histlen);
    serverLog(LL_DEBUG, "[PSYNC] Current index: %lld",
             server.repl_backlog_idx);

    /* Compute the amount of bytes we need to discard. */
    skip = offset - server.repl_backlog_off;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);

    /* Point j to the oldest byte, that is actually our
     * server.repl_backlog_off byte. */
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    serverLog(LL_DEBUG, "[PSYNC] Index of first byte: %lld", j);

    /* Discard the amount of data to seek to the specified 'offset'. */
    j = (j + skip) % server.repl_backlog_size;

    /* Feed replica with data. Since it is a circular buffer we have to
     * split the reply in two parts if we are cross-boundary. */
    len = server.repl_backlog_histlen - skip;
    serverLog(LL_DEBUG, "[PSYNC] Reply total length: %lld", len);
    while(len) {
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;

        serverLog(LL_DEBUG, "[PSYNC] addReply() length: %lld", thislen);
        addReplySds(c,sdsnewlen(server.repl_backlog + j, thislen));
        len -= thislen;
        j = 0;
    }
    return server.repl_backlog_histlen - skip;
}

/* Return the offset to provide as reply to the PSYNC command received
 * from the replica. The returned value is only valid immediately after
 * the BGSAVE process started and before executing any other command
 * from clients. */
long long getPsyncInitialOffset(void) {
    return server.primary_repl_offset;
}

/* Send a FULLRESYNC reply in the specific case of a full resynchronization,
 * as a side effect setup the replica for a full sync in different ways:
 *
 * 1) Remember, into the replica client structure, the replication offset
 *    we sent here, so that if new replicas will later attach to the same
 *    background RDB saving process (by duplicating this client output
 *    buffer), we can get the right offset from this replica.
 * 2) Set the replication state of the replica to WAIT_BGSAVE_END so that
 *    we start accumulating differences from this point.
 * 3) Force the replication stream to re-emit a SELECT statement so
 *    the new replica incremental differences will start selecting the
 *    right database number.
 *
 * Normally this function should be called immediately after a successful
 * BGSAVE for replication was started, or when there is one already in
 * progress that we attached our replica to. */
int replicationSetupReplicaForFullResync(client *replica, long long offset) {
    char buf[128];
    int buflen;

    replica->psync_initial_offset = offset;
    replica->replstate = REPLICA_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * replica as well. Set replicaseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    server.replicaseldb = -1;

    /* Don't send this reply to replicas that approached us with
     * the old SYNC command. */
    if (!(replica->flags & CLIENT_PRE_PSYNC)) {
        buflen = snprintf(buf,sizeof(buf),"+FULLRESYNC %s %lld\r\n",
                          server.replid,offset);
        if (write(replica->fd,buf,buflen) != buflen) {
            freeClientAsync(replica);
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function handles the PSYNC command from the point of view of a
 * primary receiving a request for partial resynchronization.
 *
 * On success return C_OK, otherwise C_ERR is returned and we proceed
 * with the usual full resync. */
int primaryTryPartialResynchronization(client *c) {
    long long psync_offset, psync_len;
    char *primary_replid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    /* Parse the replication offset asked by the replica. Go to full sync
     * on parse error: this should never happen but we try to handle
     * it in a robust way compared to aborting. */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&psync_offset,NULL) !=
       C_OK) goto need_full_resync;

    /* Is the replication ID of this primary the same advertised by the wannabe
     * replica via PSYNC? If the replication ID changed this primary has a
     * different replication history, and there is no way to continue.
     *
     * Note that there are two potentially valid replication IDs: the ID1
     * and the ID2. The ID2 however is only valid up to a specific offset. */
    if (strcasecmp(primary_replid, server.replid) &&
        (strcasecmp(primary_replid, server.replid2) ||
         psync_offset > server.second_replid_offset))
    {
        /* Run id "?" is used by replicas that want to force a full resync. */
        if (primary_replid[0] != '?') {
            if (strcasecmp(primary_replid, server.replid) &&
                strcasecmp(primary_replid, server.replid2))
            {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Replication ID mismatch (Replica asked for '%s', my "
                    "replication IDs are '%s' and '%s')",
                    primary_replid, server.replid, server.replid2);
            } else {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Requested offset for second ID was %lld, but I can reply "
                    "up to %lld", psync_offset, server.second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE,"Full resync requested by replica %s",
                replicationGetReplicaName(c));
        }
        goto need_full_resync;
    }

    /* We still have the data our replica is asking for? */
    if (!server.repl_backlog ||
        psync_offset < server.repl_backlog_off ||
        psync_offset > (server.repl_backlog_off + server.repl_backlog_histlen))
    {
        serverLog(LL_NOTICE,
            "Unable to partial resync with replica %s for lack of backlog (Replica request was: %lld).", replicationGetReplicaName(c), psync_offset);
        if (psync_offset > server.primary_repl_offset) {
            serverLog(LL_WARNING,
                "Warning: replica %s tried to PSYNC with an offset that is greater than the primary replication offset.", replicationGetReplicaName(c));
        }
        goto need_full_resync;
    }

    /* If we reached this point, we are able to perform a partial resync:
     * 1) Set client state to make it a replica.
     * 2) Inform the client we can continue with +CONTINUE
     * 3) Send the backlog data (from the offset to the end) to the replica. */
    c->flags |= CLIENT_REPLICA;
    c->replstate = REPLICA_STATE_ONLINE;
    c->repl_ack_time = server.unixtime;
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.replicas,c);
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * empty so this write will never fail actually. */
    if (c->replica_capa & REPLICA_CAPA_PSYNC2) {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n", server.replid);
    } else {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE\r\n");
    }
    if (write(c->fd,buf,buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }
    psync_len = addReplyReplicationBacklog(c,psync_offset);
    serverLog(LL_NOTICE,
        "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
            replicationGetReplicaName(c),
            psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at server.replicaseldb
     * to -1 to force the primary to emit SELECT, since the replica already
     * has this state from the previous connection with the primary. */

    refreshGoodReplicasCount();
    return C_OK; /* The caller can return, no full resync needed. */

need_full_resync:
    /* We need a full resync for some reason... Note that we can't
     * reply to PSYNC right now if a full SYNC is needed. The reply
     * must include the primary offset at the time the RDB file we transfer
     * is generated, so we need to delay the reply to that moment. */
    return C_ERR;
}

/* Start a BGSAVE for replication goals, which is, selecting the disk or
 * socket target depending on the configuration, and making sure that
 * the script cache is flushed before to start.
 *
 * The mincapa argument is the bitwise AND among all the replicas capabilities
 * of the replicas waiting for this BGSAVE, so represents the replica capabilities
 * all the replicas support. Can be tested via REPLICA_CAPA_* macros.
 *
 * Side effects, other than starting a BGSAVE:
 *
 * 1) Handle the replicas in WAIT_START state, by preparing them for a full
 *    sync if the BGSAVE was successfully started, or sending them an error
 *    and dropping them from the list of replicas.
 *
 * 2) Flush the Lua scripting script cache if the BGSAVE was actually
 *    started.
 *
 * Returns C_OK on success or C_ERR otherwise. */
int startBgsaveForReplication(int mincapa) {
    int retval;
    int socket_target = server.repl_diskless_sync && (mincapa & REPLICA_CAPA_EOF);
    listIter li;
    listNode *ln;

    serverLog(LL_NOTICE,"Starting BGSAVE for SYNC with target: %s",
        socket_target ? "replicas sockets" : "disk");

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    /* Only do rdbSave* when rsiptr is not NULL,
     * otherwise replica will miss repl-stream-db. */
    if (rsiptr) {
        if (socket_target)
            retval = rdbSaveToReplicasSockets(rsiptr);
        else
            retval = rdbSaveBackground(server.rdb_filename,rsiptr);
    } else {
        serverLog(LL_WARNING,"BGSAVE for replication: replication information not available, can't generate the RDB file right now. Try later.");
        retval = C_ERR;
    }

    /* If we failed to BGSAVE, remove the replicas waiting for a full
     * resynchorinization from the list of salves, inform them with
     * an error about what happened, close the connection ASAP. */
    if (retval == C_ERR) {
        serverLog(LL_WARNING,"BGSAVE for replication failed");
        listRewind(server.replicas,&li);
        while((ln = listNext(&li))) {
            client *replica = ln->value;

            if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_START) {
                replica->flags &= ~CLIENT_REPLICA;
                listDelNode(server.replicas,ln);
                addReplyError(replica,
                    "BGSAVE failed, replication can't continue");
                replica->flags |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }
        return retval;
    }

    /* If the target is socket, rdbSaveToReplicasSockets() already setup
     * the salves for a full resync. Otherwise for disk target do it now.*/
    if (!socket_target) {
        listRewind(server.replicas,&li);
        while((ln = listNext(&li))) {
            client *replica = ln->value;

            if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_START) {
                    replicationSetupReplicaForFullResync(replica,
                            getPsyncInitialOffset());
            }
        }
    }

    /* Flush the script cache, since we need that replica differences are
     * accumulated without requiring replicas to match our cached scripts. */
    if (retval == C_OK) replicationScriptCacheFlush();
    return retval;
}

/* SYNC and PSYNC command implemenation. */
void syncCommand(client *c) {
    /* ignore SYNC if already replica or in monitor mode */
    if (c->flags & CLIENT_REPLICA) return;

    /* Refuse SYNC requests if we are a replica but the link with our primary
     * is not ok... */
    if (server.primaryhost && server.repl_state != REPL_STATE_CONNECTED) {
        addReplySds(c,sdsnew("-NOPRIMARYLINK Can't SYNC while not connected with my primary\r\n"));
        return;
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other replicas if needed. */
    if (clientHasPendingReplies(c)) {
        addReplyError(c,"SYNC and PSYNC are invalid with pending output");
        return;
    }

    serverLog(LL_NOTICE,"Replica %s asks for synchronization",
        replicationGetReplicaName(c));

    /* Try a partial resynchronization if this is a PSYNC command.
     * If it fails, we continue with usual full resynchronization, however
     * when this happens primaryTryPartialResynchronization() already
     * replied with:
     *
     * +FULLRESYNC <replid> <offset>
     *
     * So the replica knows the new replid and offset to try a PSYNC later
     * if the connection with the primary is lost. */
    if (!strcasecmp(c->argv[0]->ptr,"psync")) {
        if (primaryTryPartialResynchronization(c) == C_OK) {
            server.stat_sync_partial_ok++;
            return; /* No full resync needed, return. */
        } else {
            char *primary_replid = c->argv[1]->ptr;

            /* Increment stats for failed PSYNCs, but only if the
             * replid is not "?", as this is used by replicas to force a full
             * resync on purpose when they are not albe to partially
             * resync. */
            if (primary_replid[0] != '?') server.stat_sync_partial_err++;
        }
    } else {
        /* If a replica uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like redis-cli --replica). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        c->flags |= CLIENT_PRE_PSYNC;
    }

    /* Full resynchronization. */
    server.stat_sync_full++;

    /* Setup the replica as one waiting for BGSAVE to start. The following code
     * paths will change the state if we handle the replica differently. */
    c->replstate = REPLICA_STATE_WAIT_BGSAVE_START;
    if (server.repl_disable_tcp_nodelay)
        anetDisableTcpNoDelay(NULL, c->fd); /* Non critical if it fails. */
    c->repldbfd = -1;
    c->flags |= CLIENT_REPLICA;
    listAddNodeTail(server.replicas,c);

    /* Create the replication backlog if needed. */
    if (listLength(server.replicas) == 1 && server.repl_backlog == NULL) {
        /* When we create the backlog from scratch, we always use a new
         * replication ID and clear the ID2, since there is no valid
         * past history. */
        changeReplicationId();
        clearReplicationId2();
        createReplicationBacklog();
    }

    /* CASE 1: BGSAVE is in progress, with disk target. */
    if (server.rdb_child_pid != -1 &&
        server.rdb_child_type == RDB_CHILD_TYPE_DISK)
    {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another replica that is
         * registering differences since the server forked to save. */
        client *replica;
        listNode *ln;
        listIter li;

        listRewind(server.replicas,&li);
        while((ln = listNext(&li))) {
            replica = ln->value;
            if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_END) break;
        }
        /* To attach this replica, we check that it has at least all the
         * capabilities of the replica that triggered the current BGSAVE. */
        if (ln && ((c->replica_capa & replica->replica_capa) == replica->replica_capa)) {
            /* Perfect, the server is already registering differences for
             * another replica. Set the right state, and copy the buffer. */
            copyClientOutputBuffer(c,replica);
            replicationSetupReplicaForFullResync(c,replica->psync_initial_offset);
            serverLog(LL_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences. */
            serverLog(LL_NOTICE,"Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

    /* CASE 2: BGSAVE is in progress, with socket target. */
    } else if (server.rdb_child_pid != -1 &&
               server.rdb_child_type == RDB_CHILD_TYPE_SOCKET)
    {
        /* There is an RDB child process but it is writing directly to
         * children sockets. We need to wait for the next BGSAVE
         * in order to synchronize. */
        serverLog(LL_NOTICE,"Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC");

    /* CASE 3: There is no BGSAVE is progress. */
    } else {
        if (server.repl_diskless_sync && (c->replica_capa & REPLICA_CAPA_EOF)) {
            /* Diskless replication RDB child is created inside
             * replicationCron() since we want to delay its start a
             * few seconds to wait for more replicas to arrive. */
            if (server.repl_diskless_sync_delay)
                serverLog(LL_NOTICE,"Delay next BGSAVE for diskless SYNC");
        } else {
            /* Target is disk (or the replica is not capable of supporting
             * diskless replication) and we don't have a BGSAVE in progress,
             * let's start one. */
            if (server.aof_child_pid == -1) {
                startBgsaveForReplication(c->replica_capa);
            } else {
                serverLog(LL_NOTICE,
                    "No BGSAVE in progress, but an AOF rewrite is active. "
                    "BGSAVE for replication delayed");
            }
        }
    }
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a replica in order to configure the replication
 * process before starting it with the SYNC command.
 *
 * Currently the only use of this command is to communicate to the primary
 * what is the listening port of the Replica redis instance, so that the
 * primary can accurately list replicas and their listening ports in
 * the INFO output.
 *
 * In the future the same command can be used in order to configure
 * the replication to initiate an incremental replication instead of a
 * full resync. */
void replconfCommand(client *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j+=2) {
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != C_OK))
                return;
            c->replica_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr,"ip-address")) {
            sds ip = c->argv[j+1]->ptr;
            if (sdslen(ip) < sizeof(c->replica_ip)) {
                memcpy(c->replica_ip,ip,sdslen(ip)+1);
            } else {
                addReplyErrorFormat(c,"REPLCONF ip-address provided by "
                    "replica instance is too long: %zd bytes", sdslen(ip));
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr,"capa")) {
            /* Ignore capabilities not understood by this primary. */
            if (!strcasecmp(c->argv[j+1]->ptr,"eof"))
                c->replica_capa |= REPLICA_CAPA_EOF;
            else if (!strcasecmp(c->argv[j+1]->ptr,"psync2"))
                c->replica_capa |= REPLICA_CAPA_PSYNC2;
        } else if (!strcasecmp(c->argv[j]->ptr,"ack")) {
            /* REPLCONF ACK is used by replica to inform the primary the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            long long offset;

            if (!(c->flags & CLIENT_REPLICA)) return;
            if ((getLongLongFromObject(c->argv[j+1], &offset) != C_OK))
                return;
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            c->repl_ack_time = server.unixtime;
            /* If this was a diskless replication, we need to really put
             * the replica online when the first ACK is received (which
             * confirms replica is online and ready to get more data). */
            if (c->repl_put_online_on_ack && c->replstate == REPLICA_STATE_ONLINE)
                putReplicaOnline(c);
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the replica. */
            if (server.primaryhost && server.primary) replicationSendAck();
            return;
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c,shared.ok);
}

/* This function puts a replica in the online state, and should be called just
 * after a replica received the RDB file for the initial synchronization, and
 * we are finally ready to send the incremental stream of commands.
 *
 * It does a few things:
 *
 * 1) Put the replica in ONLINE state (useless when the function is called
 *    because state is already ONLINE but repl_put_online_on_ack is true).
 * 2) Make sure the writable event is re-installed, since calling the SYNC
 *    command disables it, so that we can accumulate output buffer without
 *    sending it to the replica.
 * 3) Update the count of good replicas. */
void putReplicaOnline(client *replica) {
    replica->replstate = REPLICA_STATE_ONLINE;
    replica->repl_put_online_on_ack = 0;
    replica->repl_ack_time = server.unixtime; /* Prevent false timeout. */
    if (aeCreateFileEvent(server.el, replica->fd, AE_WRITABLE,
        sendReplyToClient, replica) == AE_ERR) {
        serverLog(LL_WARNING,"Unable to register writable event for replica bulk transfer: %s", strerror(errno));
        freeClient(replica);
        return;
    }
    refreshGoodReplicasCount();
    serverLog(LL_NOTICE,"Synchronization with replica %s succeeded",
        replicationGetReplicaName(replica));
}

void sendBulkToReplica(aeEventLoop *el, int fd, void *privdata, int mask) {
    client *replica = privdata;
    UNUSED(el);
    UNUSED(mask);
    char buf[PROTO_IOBUF_LEN];
    ssize_t nwritten, buflen;

    /* Before sending the RDB file, we send the preamble as configured by the
     * replication process. Currently the preamble is just the bulk count of
     * the file in the form "$<length>\r\n". */
    if (replica->replpreamble) {
        nwritten = write(fd,replica->replpreamble,sdslen(replica->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_VERBOSE,"Write error sending RDB preamble to replica: %s",
                strerror(errno));
            freeClient(replica);
            return;
        }
        server.stat_net_output_bytes += nwritten;
        sdsrange(replica->replpreamble,nwritten,-1);
        if (sdslen(replica->replpreamble) == 0) {
            sdsfree(replica->replpreamble);
            replica->replpreamble = NULL;
            /* fall through sending data. */
        } else {
            return;
        }
    }

    /* If the preamble was already transferred, send the RDB bulk data. */
    lseek(replica->repldbfd,replica->repldboff,SEEK_SET);
    buflen = read(replica->repldbfd,buf,PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING,"Read error sending DB to replica: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(replica);
        return;
    }
    if ((nwritten = write(fd,buf,buflen)) == -1) {
        if (errno != EAGAIN) {
            serverLog(LL_WARNING,"Write error sending DB to replica: %s",
                strerror(errno));
            freeClient(replica);
        }
        return;
    }
    replica->repldboff += nwritten;
    server.stat_net_output_bytes += nwritten;
    if (replica->repldboff == replica->repldbsize) {
        close(replica->repldbfd);
        replica->repldbfd = -1;
        aeDeleteFileEvent(server.el,replica->fd,AE_WRITABLE);
        putReplicaOnline(replica);
    }
}

/* This function is called at the end of every background saving,
 * or when the replication RDB transfer strategy is modified from
 * disk to socket or the other way around.
 *
 * The goal of this function is to handle replicas waiting for a successful
 * background saving in order to perform non-blocking synchronization, and
 * to schedule a new BGSAVE if there are replicas that attached while a
 * BGSAVE was in progress, but it was not a good one for replication (no
 * other replica was accumulating differences).
 *
 * The argument bgsaveerr is C_OK if the background saving succeeded
 * otherwise C_ERR is passed to the function.
 * The 'type' argument is the type of the child that terminated
 * (if it had a disk or socket target). */
void updateReplicasWaitingBgsave(int bgsaveerr, int type) {
    listNode *ln;
    int startbgsave = 0;
    int mincapa = -1;
    listIter li;

    listRewind(server.replicas,&li);
    while((ln = listNext(&li))) {
        client *replica = ln->value;

        if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_START) {
            startbgsave = 1;
            mincapa = (mincapa == -1) ? replica->replica_capa :
                                        (mincapa & replica->replica_capa);
        } else if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            /* If this was an RDB on disk save, we have to prepare to send
             * the RDB from disk to the replica socket. Otherwise if this was
             * already an RDB -> Replicas socket transfer, used in the case of
             * diskless replication, our work is trivial, we can just put
             * the replica online. */
            if (type == RDB_CHILD_TYPE_SOCKET) {
                serverLog(LL_NOTICE,
                    "Streamed RDB transfer with replica %s succeeded (socket). Waiting for REPLCONF ACK from replica to enable streaming",
                        replicationGetReplicaName(replica));
                /* Note: we wait for a REPLCONF ACK message from replica in
                 * order to really put it online (install the write handler
                 * so that the accumulated data can be transferred). However
                 * we change the replication state ASAP, since our replica
                 * is technically online now. */
                replica->replstate = REPLICA_STATE_ONLINE;
                replica->repl_put_online_on_ack = 1;
                replica->repl_ack_time = server.unixtime; /* Timeout otherwise. */
            } else {
                if (bgsaveerr != C_OK) {
                    freeClient(replica);
                    serverLog(LL_WARNING,"SYNC failed. BGSAVE child returned an error");
                    continue;
                }
                if ((replica->repldbfd = open(server.rdb_filename,O_RDONLY)) == -1 ||
                    redis_fstat(replica->repldbfd,&buf) == -1) {
                    freeClient(replica);
                    serverLog(LL_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                    continue;
                }
                replica->repldboff = 0;
                replica->repldbsize = buf.st_size;
                replica->replstate = REPLICA_STATE_SEND_BULK;
                replica->replpreamble = sdscatprintf(sdsempty(),"$%lld\r\n",
                    (unsigned long long) replica->repldbsize);

                aeDeleteFileEvent(server.el,replica->fd,AE_WRITABLE);
                if (aeCreateFileEvent(server.el, replica->fd, AE_WRITABLE, sendBulkToReplica, replica) == AE_ERR) {
                    freeClient(replica);
                    continue;
                }
            }
        }
    }
    if (startbgsave) startBgsaveForReplication(mincapa);
}

/* Change the current instance replication ID with a new, random one.
 * This will prevent successful PSYNCs between this primary and other
 * replicas, so the command should be called when something happens that
 * alters the current story of the dataset. */
void changeReplicationId(void) {
    getRandomHexChars(server.replid,CONFIG_RUN_ID_SIZE);
    server.replid[CONFIG_RUN_ID_SIZE] = '\0';
}

/* Clear (invalidate) the secondary replication ID. This happens, for
 * example, after a full resynchronization, when we start a new replication
 * history. */
void clearReplicationId2(void) {
    memset(server.replid2,'0',sizeof(server.replid));
    server.replid2[CONFIG_RUN_ID_SIZE] = '\0';
    server.second_replid_offset = -1;
}

/* Use the current replication ID / offset as secondary replication
 * ID, and change the current one in order to start a new history.
 * This should be used when an instance is switched from replica to primary
 * so that it can serve PSYNC requests performed using the primary
 * replication ID. */
void shiftReplicationId(void) {
    memcpy(server.replid2,server.replid,sizeof(server.replid));
    /* We set the second replid offset to the primary offset + 1, since
     * the replica will ask for the first byte it has not yet received, so
     * we need to add one to the offset: for example if, as a replica, we are
     * sure we have the same history as the primary for 50 bytes, after we
     * are turned into a primary, we can accept a PSYNC request with offset
     * 51, since the replica asking has the same history up to the 50th
     * byte, and is asking for the new bytes starting at offset 51. */
    server.second_replid_offset = server.primary_repl_offset+1;
    changeReplicationId();
    serverLog(LL_WARNING,"Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s", server.replid2, server.second_replid_offset, server.replid);
}

/* ----------------------------------- REPLICA -------------------------------- */

/* Returns 1 if the given replication state is a handshake state,
 * 0 otherwise. */
int replicaIsInHandshakeState(void) {
    return server.repl_state >= REPL_STATE_RECEIVE_PONG &&
           server.repl_state <= REPL_STATE_RECEIVE_PSYNC;
}

/* Avoid the primary to detect the replica is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entirely or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the primary. */
void replicationSendNewlineToPrimary(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        if (write(server.repl_transfer_s,"\n",1) == -1) {
            /* Pinging back in this stage is best-effort. */
        }
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the primary. */
void replicationEmptyDbCallback(void *privdata) {
    UNUSED(privdata);
    replicationSendNewlineToPrimary();
}

/* Once we have a link with the primary and the synchroniziation was
 * performed, this function materializes the primary client we store
 * at server.primary, starting from the specified file descriptor. */
void replicationCreatePrimaryClient(int fd, int dbid) {
    server.primary = createClient(fd);
    server.primary->flags |= CLIENT_PRIMARY;
    server.primary->authenticated = 1;
    server.primary->reploff = server.primary_initial_offset;
    server.primary->read_reploff = server.primary->reploff;
    memcpy(server.primary->replid, server.primary_replid,
        sizeof(server.primary_replid));
    /* If primary offset is set to -1, this primary is old and is not
     * PSYNC capable, so we flag it accordingly. */
    if (server.primary->reploff == -1)
        server.primary->flags |= CLIENT_PRE_PSYNC;
    if (dbid != -1) selectDb(server.primary,dbid);
}

void restartAOF() {
    int retry = 10;
    while (retry-- && startAppendOnly() == C_ERR) {
        serverLog(LL_WARNING,"Failed enabling the AOF after successful primary synchronization! Trying it again in one second.");
        sleep(1);
    }
    if (!retry) {
        serverLog(LL_WARNING,"FATAL: this replica instance finished the synchronization with its primary, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

/* Asynchronously read the SYNC payload we receive from a primary */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
void readSyncBulkPayload(aeEventLoop *el, int fd, void *privdata, int mask) {
    char buf[4096];
    ssize_t nread, readlen, nwritten;
    off_t left;
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);

    /* Static vars used to hold the EOF mark, and the last bytes received
     * form the server: when they match, we reached the end of the transfer. */
    static char eofmark[CONFIG_RUN_ID_SIZE];
    static char lastbytes[CONFIG_RUN_ID_SIZE];
    static int usemark = 0;

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the primary reply. */
    if (server.repl_transfer_size == -1) {
        if (syncReadLine(fd,buf,1024,server.repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,
                "I/O error reading bulk count from PRIMARY: %s",
                strerror(errno));
            goto error;
        }

        if (buf[0] == '-') {
            serverLog(LL_WARNING,
                "PRIMARY aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            serverLog(LL_WARNING,"Bad protocol from PRIMARY, the first byte is not '$' (we received '%s'), are you sure the host and port are right?", buf);
            goto error;
        }

        /* There are two possible forms for the bulk payload. One is the
         * usual $<count> bulk format. The other is used for diskless transfers
         * when the primary does not know beforehand the size of the file to
         * transfer. In the latter case, the following format is used:
         *
         * $EOF:<40 bytes delimiter>
         *
         * At the end of the file the announced delimiter is transmitted. The
         * delimiter is long and random enough that the probability of a
         * collision with the actual file content can be ignored. */
        if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= CONFIG_RUN_ID_SIZE) {
            usemark = 1;
            memcpy(eofmark,buf+5,CONFIG_RUN_ID_SIZE);
            memset(lastbytes,0,CONFIG_RUN_ID_SIZE);
            /* Set any repl_transfer_size to avoid entering this code path
             * at the next call. */
            server.repl_transfer_size = 0;
            serverLog(LL_NOTICE,
                "PRIMARY <-> REPLICA sync: receiving streamed RDB from primary");
        } else {
            usemark = 0;
            server.repl_transfer_size = strtol(buf+1,NULL,10);
            serverLog(LL_NOTICE,
                "PRIMARY <-> REPLICA sync: receiving %lld bytes from primary",
                (long long) server.repl_transfer_size);
        }
        return;
    }

    /* Read bulk data */
    if (usemark) {
        readlen = sizeof(buf);
    } else {
        left = server.repl_transfer_size - server.repl_transfer_read;
        readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
    }

    nread = read(fd,buf,readlen);
    if (nread <= 0) {
        serverLog(LL_WARNING,"I/O error trying to sync with PRIMARY: %s",
            (nread == -1) ? strerror(errno) : "connection lost");
        cancelReplicationHandshake();
        return;
    }
    server.stat_net_input_bytes += nread;

    /* When a mark is used, we want to detect EOF asap in order to avoid
     * writing the EOF mark into the file... */
    int eof_reached = 0;

    if (usemark) {
        /* Update the last bytes array, and check if it matches our delimiter.*/
        if (nread >= CONFIG_RUN_ID_SIZE) {
            memcpy(lastbytes,buf+nread-CONFIG_RUN_ID_SIZE,CONFIG_RUN_ID_SIZE);
        } else {
            int rem = CONFIG_RUN_ID_SIZE-nread;
            memmove(lastbytes,lastbytes+nread,rem);
            memcpy(lastbytes+rem,buf,nread);
        }
        if (memcmp(lastbytes,eofmark,CONFIG_RUN_ID_SIZE) == 0) eof_reached = 1;
    }

    server.repl_transfer_lastio = server.unixtime;
    if ((nwritten = write(server.repl_transfer_fd,buf,nread)) != nread) {
        serverLog(LL_WARNING,"Write error or short write writing to the DB dump file needed for PRIMARY <-> REPLICA synchronization: %s", 
            (nwritten == -1) ? strerror(errno) : "short write");
        goto error;
    }
    server.repl_transfer_read += nread;

    /* Delete the last 40 bytes from the file if we reached EOF. */
    if (usemark && eof_reached) {
        if (ftruncate(server.repl_transfer_fd,
            server.repl_transfer_read - CONFIG_RUN_ID_SIZE) == -1)
        {
            serverLog(LL_WARNING,"Error truncating the RDB file received from the primary for SYNC: %s", strerror(errno));
            goto error;
        }
    }

    /* Sync data on disk from time to time, otherwise at the end of the transfer
     * we may suffer a big delay as the memory buffers are copied into the
     * actual disk. */
    if (server.repl_transfer_read >=
        server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
    {
        off_t sync_size = server.repl_transfer_read -
                          server.repl_transfer_last_fsync_off;
        rdb_fsync_range(server.repl_transfer_fd,
            server.repl_transfer_last_fsync_off, sync_size);
        server.repl_transfer_last_fsync_off += sync_size;
    }

    /* Check if the transfer is now complete */
    if (!usemark) {
        if (server.repl_transfer_read == server.repl_transfer_size)
            eof_reached = 1;
    }

    if (eof_reached) {
        int aof_is_enabled = server.aof_state != AOF_OFF;

        if (rename(server.repl_transfer_tmpfile,server.rdb_filename) == -1) {
            serverLog(LL_WARNING,"Failed trying to rename the temp DB into dump.rdb in PRIMARY <-> REPLICA synchronization: %s", strerror(errno));
            cancelReplicationHandshake();
            return;
        }
        serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Flushing old data");
        /* We need to stop any AOFRW fork before flusing and parsing
         * RDB, otherwise we'll create a copy-on-write disaster. */
        if(aof_is_enabled) stopAppendOnly();
        signalFlushedDb(-1);
        emptyDb(
            -1,
            server.repl_replica_lazy_flush ? EMPTYDB_ASYNC : EMPTYDB_NO_FLAGS,
            replicationEmptyDbCallback);
        /* Before loading the DB into memory we need to delete the readable
         * handler, otherwise it will get called recursively since
         * rdbLoad() will call the event loop to process events from time to
         * time for non blocking loading. */
        aeDeleteFileEvent(server.el,server.repl_transfer_s,AE_READABLE);
        serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Loading DB in memory");
        rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
        if (rdbLoad(server.rdb_filename,&rsi) != C_OK) {
            serverLog(LL_WARNING,"Failed trying to load the PRIMARY synchronization DB from disk");
            cancelReplicationHandshake();
            /* Re-enable the AOF if we disabled it earlier, in order to restore
             * the original configuration. */
            if (aof_is_enabled) restartAOF();
            return;
        }
        /* Final setup of the connected replica <- primary link */
        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);
        replicationCreatePrimaryClient(server.repl_transfer_s,rsi.repl_stream_db);
        server.repl_state = REPL_STATE_CONNECTED;
        server.repl_down_since = 0;
        /* After a full resynchroniziation we use the replication ID and
         * offset of the primary. The secondary ID / offset are cleared since
         * we are starting a new history. */
        memcpy(server.replid,server.primary->replid,sizeof(server.replid));
        server.primary_repl_offset = server.primary->reploff;
        clearReplicationId2();
        /* Let's create the replication backlog if needed. Replicas need to
         * accumulate the backlog regardless of the fact they have sub-replicas
         * or not, in order to behave correctly if they are promoted to
         * primaries after a failover. */
        if (server.repl_backlog == NULL) createReplicationBacklog();

        serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Finished with success");
        /* Restart the AOF subsystem now that we finished the sync. This
         * will trigger an AOF rewrite, and when done will start appending
         * to the new file. */
        if (aof_is_enabled) restartAOF();
    }
    return;

error:
    cancelReplicationHandshake();
    return;
}

/* Send a synchronous command to the primary. Used to send AUTH and
 * REPLCONF commands before starting the replication with SYNC.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
#define SYNC_CMD_READ (1<<0)
#define SYNC_CMD_WRITE (1<<1)
#define SYNC_CMD_FULL (SYNC_CMD_READ|SYNC_CMD_WRITE)
char *sendSynchronousCommand(int flags, int fd, ...) {

    /* Create the command to send to the primary, we use redis binary
     * protocol to make sure correct arguments are sent. This function
     * is not safe for all binary data. */
    if (flags & SYNC_CMD_WRITE) {
        char *arg;
        va_list ap;
        sds cmd = sdsempty();
        sds cmdargs = sdsempty();
        size_t argslen = 0;
        va_start(ap,fd);

        while(1) {
            arg = va_arg(ap, char*);
            if (arg == NULL) break;

            cmdargs = sdscatprintf(cmdargs,"$%zu\r\n%s\r\n",strlen(arg),arg);
            argslen++;
        }

        va_end(ap);

        cmd = sdscatprintf(cmd,"*%zu\r\n",argslen);
        cmd = sdscatsds(cmd,cmdargs);
        sdsfree(cmdargs);

        /* Transfer command to the server. */
        if (syncWrite(fd,cmd,sdslen(cmd),server.repl_syncio_timeout*1000)
            == -1)
        {
            sdsfree(cmd);
            return sdscatprintf(sdsempty(),"-Writing to primary: %s",
                    strerror(errno));
        }
        sdsfree(cmd);
    }

    /* Read the reply from the server. */
    if (flags & SYNC_CMD_READ) {
        char buf[256];

        if (syncReadLine(fd,buf,sizeof(buf),server.repl_syncio_timeout*1000)
            == -1)
        {
            return sdscatprintf(sdsempty(),"-Reading from primary: %s",
                    strerror(errno));
        }
        server.repl_transfer_lastio = server.unixtime;
        return sdsnew(buf);
    }
    return NULL;
}

/* Try a partial resynchronization with the primary if we are about to reconnect.
 * If there is no cached primary structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the primary run id and the primary replication
 * global offset.
 *
 * This function is designed to be called from syncWithPrimary(), so the
 * following assumptions are made:
 *
 * 1) We pass the function an already connected socket "fd".
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the server.primary client structure.
 *
 * The function is split in two halves: if read_reply is 0, the function
 * writes the PSYNC command on the socket, and a new function call is
 * needed, with read_reply set to 1, in order to read the reply of the
 * command. This is useful in order to support non blocking operations, so
 * that we write, return into the event loop, and read when there are data.
 *
 * When read_reply is 0 the function returns PSYNC_WRITE_ERR if there
 * was a write error, or PSYNC_WAIT_REPLY to signal we need another call
 * with read_reply set to 1. However even when read_reply is set to 1
 * the function may return PSYNC_WAIT_REPLY again to signal there were
 * insufficient data to read to complete its work. We should re-enter
 * into the event loop and wait in such a case.
 *
 * The function returns:
 *
 * PSYNC_CONTINUE: If the PSYNC command succeeded and we can continue.
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the primary run_id and global replication
 *                   offset is saved.
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 * PSYNC_WRITE_ERROR: There was an error writing the command to the socket.
 * PSYNC_WAIT_REPLY: Call again the function with read_reply set to 1.
 * PSYNC_TRY_LATER: Primary is currently in a transient error condition.
 *
 * Notable side effects:
 *
 * 1) As a side effect of the function call the function removes the readable
 *    event handler from "fd", unless the return value is PSYNC_WAIT_REPLY.
 * 2) server.primary_initial_offset is set to the right value according
 *    to the primary reply. This will be used to populate the 'server.primary'
 *    structure replication offset.
 */

#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
int replicaTryPartialResynchronization(int fd, int read_reply) {
    char *psync_replid;
    char psync_offset[32];
    sds reply;

    /* Writing half */
    if (!read_reply) {
        /* Initially set primary_initial_offset to -1 to mark the current
         * primary run_id and offset as not valid. Later if we'll be able to do
         * a FULL resync using the PSYNC command we'll set the offset at the
         * right value, so that this information will be propagated to the
         * client structure representing the primary into server.primary. */
        server.primary_initial_offset = -1;

        if (server.cached_primary) {
            psync_replid = server.cached_primary->replid;
            snprintf(psync_offset,sizeof(psync_offset),"%lld", server.cached_primary->reploff+1);
            serverLog(LL_NOTICE,"Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);
        } else {
            serverLog(LL_NOTICE,"Partial resynchronization not possible (no cached primary)");
            psync_replid = "?";
            memcpy(psync_offset,"-1",3);
        }

        /* Issue the PSYNC command */
        reply = sendSynchronousCommand(SYNC_CMD_WRITE,fd,"PSYNC",psync_replid,psync_offset,NULL);
        if (reply != NULL) {
            serverLog(LL_WARNING,"Unable to send PSYNC to primary: %s",reply);
            sdsfree(reply);
            aeDeleteFileEvent(server.el,fd,AE_READABLE);
            return PSYNC_WRITE_ERROR;
        }
        return PSYNC_WAIT_REPLY;
    }

    /* Reading half */
    reply = sendSynchronousCommand(SYNC_CMD_READ,fd,NULL);
    if (sdslen(reply) == 0) {
        /* The primary may send empty newlines after it receives PSYNC
         * and before to reply, just to keep the connection alive. */
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    aeDeleteFileEvent(server.el,fd,AE_READABLE);

    if (!strncmp(reply,"+FULLRESYNC",11)) {
        char *replid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the run id
         * and the replication offset. */
        replid = strchr(reply,' ');
        if (replid) {
            replid++;
            offset = strchr(replid,' ');
            if (offset) offset++;
        }
        if (!replid || !offset || (offset-replid-1) != CONFIG_RUN_ID_SIZE) {
            serverLog(LL_WARNING,
                "Primary replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the primary supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the primary
             * replid to make sure next PSYNCs will fail. */
            memset(server.primary_replid,0,CONFIG_RUN_ID_SIZE+1);
        } else {
            memcpy(server.primary_replid, replid, offset-replid-1);
            server.primary_replid[CONFIG_RUN_ID_SIZE] = '\0';
            server.primary_initial_offset = strtoll(offset,NULL,10);
            serverLog(LL_NOTICE,"Full resync from primary: %s:%lld",
                server.primary_replid,
                server.primary_initial_offset);
        }
        /* We are going to full resync, discard the cached primary structure. */
        replicationDiscardCachedPrimary();
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    if (!strncmp(reply,"+CONTINUE",9)) {
        /* Partial resync was accepted. */
        serverLog(LL_NOTICE,
            "Successful partial resynchronization with primary.");

        /* Check the new replication ID advertised by the primary. If it
         * changed, we need to set the new ID as primary ID, and set or
         * secondary ID as the old primary ID up to the current offset, so
         * that our sub-replicas will be able to PSYNC with us after a
         * disconnection. */
        char *start = reply+10;
        char *end = reply+9;
        while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end-start == CONFIG_RUN_ID_SIZE) {
            char new[CONFIG_RUN_ID_SIZE+1];
            memcpy(new,start,CONFIG_RUN_ID_SIZE);
            new[CONFIG_RUN_ID_SIZE] = '\0';

            if (strcmp(new,server.cached_primary->replid)) {
                /* Primary ID changed. */
                serverLog(LL_WARNING,"Primary replication ID changed to %s",new);

                /* Set the old ID as our ID2, up to the current offset+1. */
                memcpy(server.replid2,server.cached_primary->replid,
                    sizeof(server.replid2));
                server.second_replid_offset = server.primary_repl_offset+1;

                /* Update the cached primary ID and our own primary ID to the
                 * new one. */
                memcpy(server.replid,new,sizeof(server.replid));
                memcpy(server.cached_primary->replid,new,sizeof(server.replid));

                /* Disconnect all the sub-replicas: they need to be notified. */
                disconnectReplicas();
            }
        }

        /* Setup the replication to continue. */
        sdsfree(reply);
        replicationResurrectCachedPrimary(fd);

        /* If this instance was restarted and we read the metadata to
         * PSYNC from the persistence file, our replication backlog could
         * be still not initialized. Create it. */
        if (server.repl_backlog == NULL) createReplicationBacklog();
        return PSYNC_CONTINUE;
    }

    /* If we reach this point we received either an error (since the primary does
     * not understand PSYNC or because it is in a special state and cannot
     * serve our request), or an unexpected reply from the primary.
     *
     * Return PSYNC_NOT_SUPPORTED on errors we don't understand, otherwise
     * return PSYNC_TRY_LATER if we believe this is a transient error. */

    if (!strncmp(reply,"-NOPRIMARYLINK",13) ||
        !strncmp(reply,"-LOADING",8))
    {
        serverLog(LL_NOTICE,
            "Primary is currently unable to PSYNC "
            "but should be in the future: %s", reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    if (strncmp(reply,"-ERR",4)) {
        /* If it's not an error, log the unexpected event. */
        serverLog(LL_WARNING,
            "Unexpected reply to PSYNC from primary: %s", reply);
    } else {
        serverLog(LL_NOTICE,
            "Primary does not support PSYNC or is in "
            "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    replicationDiscardCachedPrimary();
    return PSYNC_NOT_SUPPORTED;
}

/* This handler fires when the non blocking connect was able to
 * establish a connection with the primary. */
void syncWithPrimary(aeEventLoop *el, int fd, void *privdata, int mask) {
    char tmpfile[256], *err = NULL;
    int dfd = -1, maxtries = 5;
    int sockerr = 0, psync_result;
    socklen_t errlen = sizeof(sockerr);
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);

    /* If this event fired after the user turned the instance into a primary
     * with REPLICAOF NO ONE we must just return ASAP. */
    if (server.repl_state == REPL_STATE_NONE) {
        close(fd);
        return;
    }

    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state. */
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    if (sockerr) {
        serverLog(LL_WARNING,"Error condition on socket for SYNC: %s",
            strerror(sockerr));
        goto error;
    }

    /* Send a PING to check the primary is able to reply without errors. */
    if (server.repl_state == REPL_STATE_CONNECTING) {
        serverLog(LL_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        aeDeleteFileEvent(server.el,fd,AE_WRITABLE);
        server.repl_state = REPL_STATE_RECEIVE_PONG;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        err = sendSynchronousCommand(SYNC_CMD_WRITE,fd,"PING",NULL);
        if (err) goto write_error;
        return;
    }

    /* Receive the PONG command. */
    if (server.repl_state == REPL_STATE_RECEIVE_PONG) {
        err = sendSynchronousCommand(SYNC_CMD_READ,fd,NULL);

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        if (err[0] != '+' &&
            strncmp(err,"-NOAUTH",7) != 0 &&
            strncmp(err,"-ERR operation not permitted",28) != 0)
        {
            serverLog(LL_WARNING,"Error reply to PING from primary: '%s'",err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE,
                "Primary replied to PING, replication can continue...");
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_AUTH;
    }

    /* AUTH with the primary if required. */
    if (server.repl_state == REPL_STATE_SEND_AUTH) {
        if (server.primaryauth) {
            err = sendSynchronousCommand(SYNC_CMD_WRITE,fd,"AUTH",server.primaryauth,NULL);
            if (err) goto write_error;
            server.repl_state = REPL_STATE_RECEIVE_AUTH;
            return;
        } else {
            server.repl_state = REPL_STATE_SEND_PORT;
        }
    }

    /* Receive AUTH reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH) {
        err = sendSynchronousCommand(SYNC_CMD_READ,fd,NULL);
        if (err[0] == '-') {
            serverLog(LL_WARNING,"Unable to AUTH to PRIMARY: %s",err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_PORT;
    }

    /* Set the replica port, so that Primary's INFO command can list the
     * replica listening port correctly. */
    if (server.repl_state == REPL_STATE_SEND_PORT) {
        sds port = sdsfromlonglong(server.replica_announce_port ?
            server.replica_announce_port : server.port);
        err = sendSynchronousCommand(SYNC_CMD_WRITE,fd,"REPLCONF",
                "listening-port",port, NULL);
        sdsfree(port);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_PORT;
        return;
    }

    /* Receive REPLCONF listening-port reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_PORT) {
        err = sendSynchronousCommand(SYNC_CMD_READ,fd,NULL);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Primary does not understand "
                                "REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_IP;
    }

    /* Skip REPLCONF ip-address if there is no replica-announce-ip option set. */
    if (server.repl_state == REPL_STATE_SEND_IP &&
        server.replica_announce_ip == NULL)
    {
            server.repl_state = REPL_STATE_SEND_CAPA;
    }

    /* Set the replica ip, so that Primary's INFO command can list the
     * replica IP address port correctly in case of port forwarding or NAT. */
    if (server.repl_state == REPL_STATE_SEND_IP) {
        err = sendSynchronousCommand(SYNC_CMD_WRITE,fd,"REPLCONF",
                "ip-address",server.replica_announce_ip, NULL);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_IP;
        return;
    }

    /* Receive REPLCONF ip-address reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_IP) {
        err = sendSynchronousCommand(SYNC_CMD_READ,fd,NULL);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Primary does not understand "
                                "REPLCONF ip-address: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_CAPA;
    }

    /* Inform the primary of our (replica) capabilities.
     *
     * EOF: supports EOF-style RDB transfer for diskless replication.
     * PSYNC2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
     *
     * The primary will ignore capabilities it does not understand. */
    if (server.repl_state == REPL_STATE_SEND_CAPA) {
        err = sendSynchronousCommand(SYNC_CMD_WRITE,fd,"REPLCONF",
                "capa","eof","capa","psync2",NULL);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_CAPA;
        return;
    }

    /* Receive CAPA reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_CAPA) {
        err = sendSynchronousCommand(SYNC_CMD_READ,fd,NULL);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF capa. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Primary does not understand "
                                  "REPLCONF capa: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_PSYNC;
    }

    /* Try a partial resynchonization. If we don't have a cached primary
     * replicaTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the primary run id
     * and the global offset, to try a partial resync at the next
     * reconnection attempt. */
    if (server.repl_state == REPL_STATE_SEND_PSYNC) {
        if (replicaTryPartialResynchronization(fd,0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_PSYNC;
        return;
    }

    /* If reached this point, we should be in REPL_STATE_RECEIVE_PSYNC. */
    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC) {
        serverLog(LL_WARNING,"syncWithPrimary(): state machine error, "
                             "state should be RECEIVE_PSYNC but is %d",
                             server.repl_state);
        goto error;
    }

    psync_result = replicaTryPartialResynchronization(fd,1);
    if (psync_result == PSYNC_WAIT_REPLY) return; /* Try again later... */

    /* If the primary is in an transient error, we should try to PSYNC
     * from scratch later, so go to the error path. This happens when
     * the server is loading the dataset or is not connected with its
     * primary and so forth. */
    if (psync_result == PSYNC_TRY_LATER) goto error;

    /* Note: if PSYNC does not return WAIT_REPLY, it will take care of
     * uninstalling the read handler from the file descriptor. */

    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "PRIMARY <-> REPLICA sync: Primary accepted a Partial Resynchronization.");
        return;
    }

    /* PSYNC failed or is not supported: we want our replicas to resync with us
     * as well, if we have any sub-replicas. The primary may transfer us an
     * entirely different data set and we have no way to incrementally feed
     * our replicas after that. */
    disconnectReplicas(); /* Force our replicas to resync with us as well. */
    freeReplicationBacklog(); /* Don't allow our chained replicas to PSYNC. */

    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the server.primary_replid and primary_initial_offset are
     * already populated. */
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE,"Retrying with SYNC...");
        if (syncWrite(fd,"SYNC\r\n",6,server.repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,"I/O error writing to PRIMARY: %s",
                strerror(errno));
            goto error;
        }
    }

    /* Prepare a suitable temp file for bulk transfer */
    while(maxtries--) {
        snprintf(tmpfile,256,
            "temp-%d.%ld.rdb",(int)server.unixtime,(long int)getpid());
        dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
        if (dfd != -1) break;
        sleep(1);
    }
    if (dfd == -1) {
        serverLog(LL_WARNING,"Opening the temp file needed for PRIMARY <-> REPLICA synchronization: %s",strerror(errno));
        goto error;
    }

    /* Setup the non blocking download of the bulk file. */
    if (aeCreateFileEvent(server.el,fd, AE_READABLE,readSyncBulkPayload,NULL)
            == AE_ERR)
    {
        serverLog(LL_WARNING,
            "Can't create readable event for SYNC: %s (fd=%d)",
            strerror(errno),fd);
        goto error;
    }

    server.repl_state = REPL_STATE_TRANSFER;
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_fd = dfd;
    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_tmpfile = zstrdup(tmpfile);
    return;

error:
    aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
    if (dfd != -1) close(dfd);
    close(fd);
    server.repl_transfer_s = -1;
    server.repl_state = REPL_STATE_CONNECT;
    return;

write_error: /* Handle sendSynchronousCommand(SYNC_CMD_WRITE) errors. */
    serverLog(LL_WARNING,"Sending command to primary in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}

int connectWithPrimary(void) {
    int fd;

    fd = anetTcpNonBlockBestEffortBindConnect(NULL,
        server.primaryhost,server.primaryport,NET_FIRST_BIND_ADDR);
    if (fd == -1) {
        serverLog(LL_WARNING,"Unable to connect to PRIMARY: %s",
            strerror(errno));
        return C_ERR;
    }

    if (aeCreateFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE,syncWithPrimary,NULL) ==
            AE_ERR)
    {
        close(fd);
        serverLog(LL_WARNING,"Can't create readable event for SYNC");
        return C_ERR;
    }

    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_s = fd;
    server.repl_state = REPL_STATE_CONNECTING;
    return C_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void undoConnectWithPrimary(void) {
    int fd = server.repl_transfer_s;

    aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
    close(fd);
    server.repl_transfer_s = -1;
}

/* Abort the async download of the bulk dataset while SYNC-ing with primary.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void replicationAbortSyncTransfer(void) {
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    undoConnectWithPrimary();
    close(server.repl_transfer_fd);
    unlink(server.repl_transfer_tmpfile);
    zfree(server.repl_transfer_tmpfile);
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REPL_STATE_CONNECT.
 *
 * Otherwise zero is returned and no operation is perforemd at all. */
int cancelReplicationHandshake(void) {
    if (server.repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer();
        server.repl_state = REPL_STATE_CONNECT;
    } else if (server.repl_state == REPL_STATE_CONNECTING ||
               replicaIsInHandshakeState())
    {
        undoConnectWithPrimary();
        server.repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }
    return 1;
}

/* Set replication to the specified primary address and port. */
void replicationSetPrimary(char *ip, int port) {
    int was_primary = server.primaryhost == NULL;

    sdsfree(server.primaryhost);
    server.primaryhost = sdsnew(ip);
    server.primaryport = port;
    if (server.primary) {
        freeClient(server.primary);
    }
    disconnectAllBlockedClients(); /* Clients blocked in primary, now replica. */

    /* Force our replicas to resync with us as well. They may hopefully be able
     * to partially resync with us, but we can notify the replid change. */
    disconnectReplicas();
    cancelReplicationHandshake();
    /* Before destroying our primary state, create a cached primary using
     * our own parameters, to later PSYNC with the new primary. */
    if (was_primary) replicationCachePrimaryUsingMyself();
    server.repl_state = REPL_STATE_CONNECT;
}

/* Cancel replication, setting the instance as a primary itself. */
void replicationUnsetPrimary(void) {
    if (server.primaryhost == NULL) return; /* Nothing to do. */
    sdsfree(server.primaryhost);
    server.primaryhost = NULL;
    /* When a replica is turned into a primary, the current replication ID
     * (that was inherited from the primary at synchronization time) is
     * used as secondary ID up to the current offset, and a new replication
     * ID is created to continue with a new replication history. */
    shiftReplicationId();
    if (server.primary) freeClient(server.primary);
    replicationDiscardCachedPrimary();
    cancelReplicationHandshake();
    /* Disconnecting all the replicas is required: we need to inform replicas
     * of the replication ID change (see shiftReplicationId() call). However
     * the replicas will be able to partially resync with us, so it will be
     * a very fast reconnection. */
    disconnectReplicas();
    server.repl_state = REPL_STATE_NONE;

    /* We need to make sure the new primary will start the replication stream
     * with a SELECT statement. This is forced after a full resync, but
     * with PSYNC version 2, there is no need for full resync after a
     * primary switch. */
    server.replicaseldb = -1;

    /* Once we turn from replica to primary, we consider the starting time without
     * replicas (that is used to count the replication backlog time to live) as
     * starting from now. Otherwise the backlog will be freed after a
     * failover if replicas do not connect immediately. */
    server.repl_no_replicas_since = server.unixtime;
}

/* This function is called when the replica lose the connection with the
 * primary into an unexpected way. */
void replicationHandlePrimaryDisconnection(void) {
    server.primary = NULL;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unixtime;
    /* We lost connection with our primary, don't disconnect replicas yet,
     * maybe we'll be able to PSYNC with our primary later. We'll disconnect
     * the replicas only if we'll have to do a full resync with our primary. */
}

void replicaofCommand(client *c) {
    /* REPLICAOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the primary node. */
    if (server.cluster_enabled) {
        addReplyError(c,"REPLICAOF not allowed in cluster mode.");
        return;
    }

    /* The special host/port combination "NO" "ONE" turns the instance
     * into a primary. Otherwise the new primary address is set. */
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        if (server.primaryhost) {
            replicationUnsetPrimary();
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,"PRIMARY MODE enabled (user request from '%s')",
                client);
            sdsfree(client);
        }
    } else {
        long port;

        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != C_OK))
            return;

        /* Check if we are already attached to the specified replica */
        if (server.primaryhost && !strcasecmp(server.primaryhost,c->argv[1]->ptr)
            && server.primaryport == port) {
            serverLog(LL_NOTICE,"REPLICA OF would result into synchronization with the primary we are already connected with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified primary\r\n"));
            return;
        }
        /* There was no previous primary or the user specified a different one,
         * we can continue. */
        replicationSetPrimary(c->argv[1]->ptr, port);
        sds client = catClientInfoString(sdsempty(),c);
        serverLog(LL_NOTICE,"REPLICA OF %s:%d enabled (user request from '%s')",
            server.primaryhost, server.primaryport, client);
        sdsfree(client);
    }
    addReply(c,shared.ok);
}

/* ROLE command: provide information about the role of the instance
 * (primary or replica) and additional information related to replication
 * in an easy to process format. */
void roleCommand(client *c) {
    if (server.primaryhost == NULL) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int replicas = 0;

        addReplyMultiBulkLen(c,3);
        addReplyBulkCBuffer(c,"primary",6);
        addReplyLongLong(c,server.primary_repl_offset);
        mbcount = addDeferredMultiBulkLength(c);
        listRewind(server.replicas,&li);
        while((ln = listNext(&li))) {
            client *replica = ln->value;
            char ip[NET_IP_STR_LEN], *replicaip = replica->replica_ip;

            if (replicaip[0] == '\0') {
                if (anetPeerToString(replica->fd,ip,sizeof(ip),NULL) == -1)
                    continue;
                replicaip = ip;
            }
            if (replica->replstate != REPLICA_STATE_ONLINE) continue;
            addReplyMultiBulkLen(c,3);
            addReplyBulkCString(c,replicaip);
            addReplyBulkLongLong(c,replica->replica_listening_port);
            addReplyBulkLongLong(c,replica->repl_ack_off);
            replicas++;
        }
        setDeferredMultiBulkLength(c,mbcount,replicas);
    } else {
        char *replicastate = NULL;

        addReplyMultiBulkLen(c,5);
        addReplyBulkCBuffer(c,"replica",5);
        addReplyBulkCString(c,server.primaryhost);
        addReplyLongLong(c,server.primaryport);
        if (replicaIsInHandshakeState()) {
            replicastate = "handshake";
        } else {
            switch(server.repl_state) {
            case REPL_STATE_NONE: replicastate = "none"; break;
            case REPL_STATE_CONNECT: replicastate = "connect"; break;
            case REPL_STATE_CONNECTING: replicastate = "connecting"; break;
            case REPL_STATE_TRANSFER: replicastate = "sync"; break;
            case REPL_STATE_CONNECTED: replicastate = "connected"; break;
            default: replicastate = "unknown"; break;
            }
        }
        addReplyBulkCString(c,replicastate);
        addReplyLongLong(c,server.primary ? server.primary->reploff : -1);
    }
}

/* Send a REPLCONF ACK command to the primary to inform it about the current
 * processed offset. If we are not connected with a primary, the command has
 * no effects. */
void replicationSendAck(void) {
    client *c = server.primary;

    if (c != NULL) {
        c->flags |= CLIENT_PRIMARY_FORCE_REPLY;
        addReplyMultiBulkLen(c,3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        addReplyBulkLongLong(c,c->reploff);
        c->flags &= ~CLIENT_PRIMARY_FORCE_REPLY;
    }
}

/* ---------------------- PRIMARY CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our primary's client structure after a transient disconnection.
 * It is cached into server.cached_primary and flushed away using the following
 * functions. */

/* This function is called by freeClient() in order to cache the primary
 * client structure instead of destroying it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached primary are:
 *
 * replicationDiscardCachedPrimary() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedPrimary() that is used after a successful PSYNC
 * handshake in order to reactivate the cached primary.
 */
void replicationCachePrimary(client *c) {
    serverAssert(server.primary != NULL && server.cached_primary == NULL);
    serverLog(LL_NOTICE,"Caching the disconnected primary state.");

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the primary client so that's ready to accept new commands:
     * we want to discard te non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the primary. */
    sdsclear(server.primary->querybuf);
    sdsclear(server.primary->pending_querybuf);
    server.primary->read_reploff = server.primary->reploff;
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);

    /* Save the primary. Server.primary will be set to null later by
     * replicationHandlePrimaryDisconnection(). */
    server.cached_primary = server.primary;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }

    /* Caching the primary happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.primary to NULL. */
    replicationHandlePrimaryDisconnection();
}

/* This function is called when a primary is turend into a replica, in order to
 * create from scratch a cached primary for the new client, that will allow
 * to PSYNC with the replica that was promoted as the new primary after a
 * failover.
 *
 * Assuming this instance was previously the primary instance of the new primary,
 * the new primary will accept its replication ID, and potentiall also the
 * current offset if no data was lost during the failover. So we use our
 * current replication ID and offset in order to synthesize a cached primary. */
void replicationCachePrimaryUsingMyself(void) {
    /* The primary client we create can be set to any DBID, because
     * the new primary will start its replication stream with SELECT. */
    server.primary_initial_offset = server.primary_repl_offset;
    replicationCreatePrimaryClient(-1,-1);

    /* Use our own ID / offset. */
    memcpy(server.primary->replid, server.replid, sizeof(server.replid));

    /* Set as cached primary. */
    unlinkClient(server.primary);
    server.cached_primary = server.primary;
    server.primary = NULL;
    serverLog(LL_NOTICE,"Before turning into a replica, using my primary parameters to synthesize a cached primary: I may be able to synchronize with the new primary with just a partial transfer.");
}

/* Free a cached primary, called when there are no longer the conditions for
 * a partial resync on reconnection. */
void replicationDiscardCachedPrimary(void) {
    if (server.cached_primary == NULL) return;

    serverLog(LL_NOTICE,"Discarding previously cached primary state.");
    server.cached_primary->flags &= ~CLIENT_PRIMARY;
    freeClient(server.cached_primary);
    server.cached_primary = NULL;
}

/* Turn the cached primary into the current primary, using the file descriptor
 * passed as argument as the socket for the new primary.
 *
 * This function is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from were this
 * primary left. */
void replicationResurrectCachedPrimary(int newfd) {
    server.primary = server.cached_primary;
    server.cached_primary = NULL;
    server.primary->fd = newfd;
    server.primary->flags &= ~(CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP);
    server.primary->authenticated = 1;
    server.primary->lastinteraction = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Re-add to the list of clients. */
    linkClient(server.primary);
    if (aeCreateFileEvent(server.el, newfd, AE_READABLE,
                          readQueryFromClient, server.primary)) {
        serverLog(LL_WARNING,"Error resurrecting the cached primary, impossible to add the readable handler: %s", strerror(errno));
        freeClientAsync(server.primary); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    if (clientHasPendingReplies(server.primary)) {
        if (aeCreateFileEvent(server.el, newfd, AE_WRITABLE,
                          sendReplyToClient, server.primary)) {
            serverLog(LL_WARNING,"Error resurrecting the cached primary, impossible to add the writable handler: %s", strerror(errno));
            freeClientAsync(server.primary); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-REPLICAS-TO-WRITE  --------------------------- */

/* This function counts the number of replicas with lag <= min-replicas-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected replicas with the specified lag (or less). */
void refreshGoodReplicasCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    if (!server.repl_min_replicas_to_write ||
        !server.repl_min_replicas_max_lag) return;

    listRewind(server.replicas,&li);
    while((ln = listNext(&li))) {
        client *replica = ln->value;
        time_t lag = server.unixtime - replica->repl_ack_time;

        if (replica->replstate == REPLICA_STATE_ONLINE &&
            lag <= server.repl_min_replicas_max_lag) good++;
    }
    server.repl_good_replicas_count = good;
}

/* ----------------------- REPLICATION SCRIPT CACHE --------------------------
 * The goal of this code is to keep track of scripts already sent to every
 * connected replica, in order to be able to replicate EVALSHA as it is without
 * translating it to EVAL every time it is possible.
 *
 * We use a capped collection implemented by a hash table for fast lookup
 * of scripts we can send as EVALSHA, plus a linked list that is used for
 * eviction of the oldest entry when the max number of items is reached.
 *
 * We don't care about taking a different cache for every different replica
 * since to fill the cache again is not very costly, the goal of this code
 * is to avoid that the same big script is trasmitted a big number of times
 * per second wasting bandwidth and processor speed, but it is not a problem
 * if we need to rebuild the cache from scratch from time to time, every used
 * script will need to be transmitted a single time to reappear in the cache.
 *
 * This is how the system works:
 *
 * 1) Every time a new replica connects, we flush the whole script cache.
 * 2) We only send as EVALSHA what was sent to the primary as EVALSHA, without
 *    trying to convert EVAL into EVALSHA specifically for replicas.
 * 3) Every time we trasmit a script as EVAL to the replicas, we also add the
 *    corresponding SHA1 of the script into the cache as we are sure every
 *    replica knows about the script starting from now.
 * 4) On SCRIPT FLUSH command, we replicate the command to all the replicas
 *    and at the same time flush the script cache.
 * 5) When the last replica disconnects, flush the cache.
 * 6) We handle SCRIPT LOAD as well since that's how scripts are loaded
 *    in the primary sometimes.
 */

/* Initialize the script cache, only called at startup. */
void replicationScriptCacheInit(void) {
    server.repl_scriptcache_size = 10000;
    server.repl_scriptcache_dict = dictCreate(&replScriptCacheDictType,NULL);
    server.repl_scriptcache_fifo = listCreate();
}

/* Empty the script cache. Should be called every time we are no longer sure
 * that every replica knows about all the scripts in our set, or when the
 * current AOF "context" is no longer aware of the script. In general we
 * should flush the cache:
 *
 * 1) Every time a new replica reconnects to this primary and performs a
 *    full SYNC (PSYNC does not require flushing).
 * 2) Every time an AOF rewrite is performed.
 * 3) Every time we are left without replicas at all, and AOF is off, in order
 *    to reclaim otherwise unused memory.
 */
void replicationScriptCacheFlush(void) {
    dictEmpty(server.repl_scriptcache_dict,NULL);
    listRelease(server.repl_scriptcache_fifo);
    server.repl_scriptcache_fifo = listCreate();
}

/* Add an entry into the script cache, if we reach max number of entries the
 * oldest is removed from the list. */
void replicationScriptCacheAdd(sds sha1) {
    int retval;
    sds key = sdsdup(sha1);

    /* Evict oldest. */
    if (listLength(server.repl_scriptcache_fifo) == server.repl_scriptcache_size)
    {
        listNode *ln = listLast(server.repl_scriptcache_fifo);
        sds oldest = listNodeValue(ln);

        retval = dictDelete(server.repl_scriptcache_dict,oldest);
        serverAssert(retval == DICT_OK);
        listDelNode(server.repl_scriptcache_fifo,ln);
    }

    /* Add current. */
    retval = dictAdd(server.repl_scriptcache_dict,key,NULL);
    listAddNodeHead(server.repl_scriptcache_fifo,key);
    serverAssert(retval == DICT_OK);
}

/* Returns non-zero if the specified entry exists inside the cache, that is,
 * if all the replicas are aware of this script SHA1. */
int replicationScriptCacheExists(sds sha1) {
    return dictFind(server.repl_scriptcache_dict,sha1) != NULL;
}

/* ----------------------- SYNCHRONOUS REPLICATION --------------------------
 * Redis synchronous replication design can be summarized in points:
 *
 * - Redis primaries have a global replication offset, used by PSYNC.
 * - Primary increment the offset every time new commands are sent to replicas.
 * - Replicas ping back primaries with the offset processed so far.
 *
 * So synchronous replication adds a new WAIT command in the form:
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * That returns the number of replicas that processed the query when
 * we finally have at least num_replicas, or when the timeout was
 * reached.
 *
 * The command is implemented in this way:
 *
 * - Every time a client processes a command, we remember the replication
 *   offset after sending that command to the replicas.
 * - When WAIT is called, we ask replicas to send an acknowledgement ASAP.
 *   The client is blocked at the same time (see blocked.c).
 * - Once we receive enough ACKs for a given offset or when the timeout
 *   is reached, the WAIT command is unblocked and the reply sent to the
 *   client.
 */

/* This just set a flag so that we broadcast a REPLCONF GETACK command
 * to all the replicas in the beforeSleep() function. Note that this way
 * we "group" all the clients that want to wait for synchronouns replication
 * in a given event loop iteration, and send a single GETACK for them all. */
void replicationRequestAckFromReplicas(void) {
    server.get_ack_from_replicas = 1;
}

/* Return the number of replicas that already acknowledged the specified
 * replication offset. */
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.replicas,&li);
    while((ln = listNext(&li))) {
        client *replica = ln->value;

        if (replica->replstate != REPLICA_STATE_ONLINE) continue;
        if (replica->repl_ack_off >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands). */
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    if (server.primaryhost) {
        addReplyError(c,"WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }

    /* Argument parsing. */
    if (getLongFromObjectOrReply(c,c->argv[1],&numreplicas,NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c,c->argv[2],&timeout,UNIT_MILLISECONDS)
        != C_OK) return;

    /* First try without blocking at all. */
    ackreplicas = replicationCountAcksByOffset(c->woff);
    if (ackreplicas >= numreplicas || c->flags & CLIENT_MULTI) {
        addReplyLongLong(c,ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from replicas. */
    c->bpop.timeout = timeout;
    c->bpop.reploffset = offset;
    c->bpop.numreplicas = numreplicas;
    listAddNodeTail(server.clients_waiting_acks,c);
    blockClient(c,BLOCKED_WAIT);

    /* Make sure that the server will send an ACK request to all the replicas
     * before returning to the event loop. */
    replicationRequestAckFromReplicas();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingReplicas(client *c) {
    listNode *ln = listSearchKey(server.clients_waiting_acks,c);
    serverAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks,ln);
}

/* Check if there are clients blocked in WAIT that can be unblocked since
 * we received enough ACKs from replicas. */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    int last_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(server.clients_waiting_acks,&li);
    while((ln = listNext(&li))) {
        client *c = ln->value;

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        if (last_offset && last_offset > c->bpop.reploffset &&
                           last_numreplicas > c->bpop.numreplicas)
        {
            unblockClient(c);
            addReplyLongLong(c,last_numreplicas);
        } else {
            int numreplicas = replicationCountAcksByOffset(c->bpop.reploffset);

            if (numreplicas >= c->bpop.numreplicas) {
                last_offset = c->bpop.reploffset;
                last_numreplicas = numreplicas;
                unblockClient(c);
                addReplyLongLong(c,numreplicas);
            }
        }
    }
}

/* Return the replica replication offset for this instance, that is
 * the offset for which we already processed the primary replication stream. */
long long replicationGetReplicaOffset(void) {
    long long offset = 0;

    if (server.primaryhost != NULL) {
        if (server.primary) {
            offset = server.primary->reploff;
        } else if (server.cached_primary) {
            offset = server.cached_primary->reploff;
        }
    }
    /* offset may be -1 when the primary does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the primary, so we return a positive
     * integer. */
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

/* Replication cron function, called 1 time per second. */
void replicationCron(void) {
    static long long replication_cron_loops = 0;

    /* Non blocking connection timeout? */
    if (server.primaryhost &&
        (server.repl_state == REPL_STATE_CONNECTING ||
         replicaIsInHandshakeState()) &&
         (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout connecting to the PRIMARY...");
        cancelReplicationHandshake();
    }

    /* Bulk transfer I/O timeout? */
    if (server.primaryhost && server.repl_state == REPL_STATE_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout receiving bulk data from PRIMARY... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        cancelReplicationHandshake();
    }

    /* Timed out primary when we are an already connected replica? */
    if (server.primaryhost && server.repl_state == REPL_STATE_CONNECTED &&
        (time(NULL)-server.primary->lastinteraction) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"PRIMARY timeout: no data nor PING received...");
        freeClient(server.primary);
    }

    /* Check if we should connect to a PRIMARY */
    if (server.repl_state == REPL_STATE_CONNECT) {
        serverLog(LL_NOTICE,"Connecting to PRIMARY %s:%d",
            server.primaryhost, server.primaryport);
        if (connectWithPrimary() == C_OK) {
            serverLog(LL_NOTICE,"PRIMARY <-> REPLICA sync started");
        }
    }

    /* Send ACK to primary from time to time.
     * Note that we do not send periodic acks to primaries that don't
     * support PSYNC and replication offsets. */
    if (server.primaryhost && server.primary &&
        !(server.primary->flags & CLIENT_PRE_PSYNC))
        replicationSendAck();

    /* If we have attached replicas, PING them from time to time.
     * So replicas can implement an explicit timeout to primaries, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. */
    listIter li;
    listNode *ln;
    robj *ping_argv[1];

    /* First, send PING according to ping_replica_period. */
    if ((replication_cron_loops % server.repl_ping_replica_period) == 0 &&
        listLength(server.replicas))
    {
        ping_argv[0] = createStringObject("PING",4);
        replicationFeedReplicas(server.replicas, server.replicaseldb,
            ping_argv, 1);
        decrRefCount(ping_argv[0]);
    }

    /* Second, send a newline to all the replicas in pre-synchronization
     * stage, that is, replicas waiting for the primary to create the RDB file.
     *
     * Also send the a newline to all the chained replicas we have, if we lost
     * connection from our primary, to keep the replicas aware that their
     * primary is online. This is needed since sub-replicas only receive proxied
     * data from top-level primaries, so there is no explicit pinging in order
     * to avoid altering the replication offsets. This special out of band
     * pings (newlines) can be sent, they will have no effect in the offset.
     *
     * The newline will be ignored by the replica but will refresh the
     * last interaction timer preventing a timeout. In this case we ignore the
     * ping period and refresh the connection once per second since certain
     * timeouts are set at a few seconds (example: PSYNC response). */
    listRewind(server.replicas,&li);
    while((ln = listNext(&li))) {
        client *replica = ln->value;

        int is_presync =
            (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_START ||
            (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_END &&
             server.rdb_child_type != RDB_CHILD_TYPE_SOCKET));

        if (is_presync) {
            if (write(replica->fd, "\n", 1) == -1) {
                /* Don't worry about socket errors, it's just a ping. */
            }
        }
    }

    /* Disconnect timedout replicas. */
    if (listLength(server.replicas)) {
        listIter li;
        listNode *ln;

        listRewind(server.replicas,&li);
        while((ln = listNext(&li))) {
            client *replica = ln->value;

            if (replica->replstate != REPLICA_STATE_ONLINE) continue;
            if (replica->flags & CLIENT_PRE_PSYNC) continue;
            if ((server.unixtime - replica->repl_ack_time) > server.repl_timeout)
            {
                serverLog(LL_WARNING, "Disconnecting timedout replica: %s",
                    replicationGetReplicaName(replica));
                freeClient(replica);
            }
        }
    }

    /* If this is a primary without attached replicas and there is a replication
     * backlog active, in order to reclaim memory we can free it after some
     * (configured) time. Note that this cannot be done for replicas: replicas
     * without sub-replicas attached should still accumulate data into the
     * backlog, in order to reply to PSYNC queries if they are turned into
     * primaries after a failover. */
    if (listLength(server.replicas) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog && server.primaryhost == NULL)
    {
        time_t idle = server.unixtime - server.repl_no_replicas_since;

        if (idle > server.repl_backlog_time_limit) {
            /* When we free the backlog, we always use a new
             * replication ID and clear the ID2. This is needed
             * because when there is no backlog, the primary_repl_offset
             * is not updated, but we would still retain our replication
             * ID, leading to the following problem:
             *
             * 1. We are a primary instance.
             * 2. Our replica is promoted to primary. It's repl-id-2 will
             *    be the same as our repl-id.
             * 3. We, yet as primary, receive some updates, that will not
             *    increment the primary_repl_offset.
             * 4. Later we are turned into a replica, connecto to the new
             *    primary that will accept our PSYNC request by second
             *    replication ID, but there will be data inconsistency
             *    because we received writes. */
            changeReplicationId();
            clearReplicationId2();
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                "Replication backlog freed after %d seconds "
                "without connected replicas.",
                (int) server.repl_backlog_time_limit);
        }
    }

    /* If AOF is disabled and we no longer have attached replicas, we can
     * free our Replication Script Cache as there is no need to propagate
     * EVALSHA at all. */
    if (listLength(server.replicas) == 0 &&
        server.aof_state == AOF_OFF &&
        listLength(server.repl_scriptcache_fifo) != 0)
    {
        replicationScriptCacheFlush();
    }

    /* Start a BGSAVE good for replication if we have replicas in
     * WAIT_BGSAVE_START state.
     *
     * In case of diskless replication, we make sure to wait the specified
     * number of seconds (according to configuration) so that other replicas
     * have the time to arrive before we start streaming. */
    if (server.rdb_child_pid == -1 && server.aof_child_pid == -1) {
        time_t idle, max_idle = 0;
        int replicas_waiting = 0;
        int mincapa = -1;
        listNode *ln;
        listIter li;

        listRewind(server.replicas,&li);
        while((ln = listNext(&li))) {
            client *replica = ln->value;
            if (replica->replstate == REPLICA_STATE_WAIT_BGSAVE_START) {
                idle = server.unixtime - replica->lastinteraction;
                if (idle > max_idle) max_idle = idle;
                replicas_waiting++;
                mincapa = (mincapa == -1) ? replica->replica_capa :
                                            (mincapa & replica->replica_capa);
            }
        }

        if (replicas_waiting &&
            (!server.repl_diskless_sync ||
             max_idle > server.repl_diskless_sync_delay))
        {
            /* Start the BGSAVE. The called function may start a
             * BGSAVE with socket target or disk target depending on the
             * configuration and replicas capabilities. */
            startBgsaveForReplication(mincapa);
        }
    }

    /* Refresh the number of replicas with lag <= min-replicas-max-lag. */
    refreshGoodReplicasCount();
    replication_cron_loops++; /* Incremented with frequency 1 HZ. */
}
