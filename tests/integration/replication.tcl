proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

start_server {tags {"repl"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set replica_log [srv 0 stdout]
    start_server {} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]

        # Configure the primary in order to hang waiting for the BGSAVE
        # operation, so that the replica remains in the handshake state.
        $primary config set repl-diskless-sync yes
        $primary config set repl-diskless-sync-delay 1000

        # Use a short replication timeout on the replica, so that if there
        # are no bugs the timeout is triggered in a reasonable amount
        # of time.
        $replica config set repl-timeout 5

        # Start the replication process...
        $replica replicaof $primary_host $primary_port

        test {Replica enters handshake} {
            wait_for_condition 50 1000 {
                [string match *handshake* [$replica role]]
            } else {
                fail "Replica does not enter handshake state"
            }
        }

        # But make the primary unable to send
        # the periodic newlines to refresh the connection. The replica
        # should detect the timeout.
        $primary debug sleep 10

        test {Replica is able to detect timeout during handshake} {
            wait_for_condition 50 1000 {
                [log_file_matches $replica_log "*Timeout connecting to the PRIMARY*"]
            } else {
                fail "Replica is not able to detect timeout"
            }
        }
    }
}

start_server {tags {"repl"}} {
    set A [srv 0 client]
    set A_host [srv 0 host]
    set A_port [srv 0 port]
    start_server {} {
        set B [srv 0 client]
        set B_host [srv 0 host]
        set B_port [srv 0 port]

        test {Set instance A as replica of B} {
            $A replicaof $B_host $B_port
            wait_for_condition 50 100 {
                [lindex [$A role] 0] eq {replica} &&
                [string match {*primary_link_status:up*} [$A info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }

        test {BRPOPLPUSH replication, when blocking against empty list} {
            set rd [redis_deferring_client]
            $rd brpoplpush a b 5
            r lpush a foo
            wait_for_condition 50 100 {
                [$A debug digest] eq [$B debug digest]
            } else {
                fail "Primary and replica have different digest: [$A debug digest] VS [$B debug digest]"
            }
        }

        test {BRPOPLPUSH replication, list exists} {
            set rd [redis_deferring_client]
            r lpush c 1
            r lpush c 2
            r lpush c 3
            $rd brpoplpush c d 5
            after 1000
            assert_equal [$A debug digest] [$B debug digest]
        }

        test {BLPOP followed by role change, issue #2473} {
            set rd [redis_deferring_client]
            $rd blpop foo 0 ; # Block while B is a primary

            # Turn B into primary of A
            $A replicaof no one
            $B replicaof $A_host $A_port
            wait_for_condition 50 100 {
                [lindex [$B role] 0] eq {replica} &&
                [string match {*primary_link_status:up*} [$B info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }

            # Push elements into the "foo" list of the new replica.
            # If the client is still attached to the instance, we'll get
            # a desync between the two instances.
            $A rpush foo a b c
            after 100

            wait_for_condition 50 100 {
                [$A debug digest] eq [$B debug digest] &&
                [$A lrange foo 0 -1] eq {a b c} &&
                [$B lrange foo 0 -1] eq {a b c}
            } else {
                fail "Primary and replica have different digest: [$A debug digest] VS [$B debug digest]"
            }
        }
    }
}

start_server {tags {"repl"}} {
    r set mykey foo

    start_server {} {
        test {Second server should have role primary at first} {
            s role
        } {primary}

        test {REPLICAOF should start with link status "down"} {
            r replicaof [srv -1 host] [srv -1 port]
            s primary_link_status
        } {down}

        test {The role should immediately be changed to "replica"} {
            s role
        } {replica}

        wait_for_sync r
        test {Sync should have transferred keys from primary} {
            r get mykey
        } {foo}

        test {The link status should be up} {
            s primary_link_status
        } {up}

        test {SET on the primary should immediately propagate} {
            r -1 set mykey bar

            wait_for_condition 500 100 {
                [r  0 get mykey] eq {bar}
            } else {
                fail "SET on primary did not propagated on replica"
            }
        }

        test {FLUSHALL should replicate} {
            r -1 flushall
            if {$::valgrind} {after 2000}
            list [r -1 dbsize] [r 0 dbsize]
        } {0 0}

        test {ROLE in primary reports primary with a replica} {
            set res [r -1 role]
            lassign $res role offset replicas
            assert {$role eq {primary}}
            assert {$offset > 0}
            assert {[llength $replicas] == 1}
            lassign [lindex $replicas 0] primary_host primary_port replica_offset
            assert {$replica_offset <= $offset}
        }

        test {ROLE in replica reports replica in connected state} {
            set res [r role]
            lassign $res role primary_host primary_port replica_state replica_offset
            assert {$role eq {replica}}
            assert {$replica_state eq {connected}}
        }
    }
}

foreach dl {no yes} {
    start_server {tags {"repl"}} {
        set primary [srv 0 client]
        $primary config set repl-diskless-sync $dl
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]
        set replicas {}
        set load_handle0 [start_write_load $primary_host $primary_port 3]
        set load_handle1 [start_write_load $primary_host $primary_port 5]
        set load_handle2 [start_write_load $primary_host $primary_port 20]
        set load_handle3 [start_write_load $primary_host $primary_port 8]
        set load_handle4 [start_write_load $primary_host $primary_port 4]
        start_server {} {
            lappend replicas [srv 0 client]
            start_server {} {
                lappend replicas [srv 0 client]
                start_server {} {
                    lappend replicas [srv 0 client]
                    test "Connect multiple replicas at the same time (issue #141), diskless=$dl" {
                        # Send REPLICAOF commands to replicas
                        [lindex $replicas 0] replicaof $primary_host $primary_port
                        [lindex $replicas 1] replicaof $primary_host $primary_port
                        [lindex $replicas 2] replicaof $primary_host $primary_port

                        # Wait for all the three replicas to reach the "online"
                        # state from the POV of the primary.
                        set retry 500
                        while {$retry} {
                            set info [r -3 info]
                            if {[string match {*replica0:*state=online*replica1:*state=online*replica2:*state=online*} $info]} {
                                break
                            } else {
                                incr retry -1
                                after 100
                            }
                        }
                        if {$retry == 0} {
                            error "assertion:Replicas not correctly synchronized"
                        }

                        # Wait that replicas acknowledge they are online so
                        # we are sure that DBSIZE and DEBUG DIGEST will not
                        # fail because of timing issues.
                        wait_for_condition 500 100 {
                            [lindex [[lindex $replicas 0] role] 3] eq {connected} &&
                            [lindex [[lindex $replicas 1] role] 3] eq {connected} &&
                            [lindex [[lindex $replicas 2] role] 3] eq {connected}
                        } else {
                            fail "Replicas still not connected after some time"
                        }

                        # Stop the write load
                        stop_write_load $load_handle0
                        stop_write_load $load_handle1
                        stop_write_load $load_handle2
                        stop_write_load $load_handle3
                        stop_write_load $load_handle4

                        # Make sure that replicas and primary have same
                        # number of keys
                        wait_for_condition 500 100 {
                            [$primary dbsize] == [[lindex $replicas 0] dbsize] &&
                            [$primary dbsize] == [[lindex $replicas 1] dbsize] &&
                            [$primary dbsize] == [[lindex $replicas 2] dbsize]
                        } else {
                            fail "Different number of keys between masted and replica after too long time."
                        }

                        # Check digests
                        set digest [$primary debug digest]
                        set digest0 [[lindex $replicas 0] debug digest]
                        set digest1 [[lindex $replicas 1] debug digest]
                        set digest2 [[lindex $replicas 2] debug digest]
                        assert {$digest ne 0000000000000000000000000000000000000000}
                        assert {$digest eq $digest0}
                        assert {$digest eq $digest1}
                        assert {$digest eq $digest2}
                    }
               }
            }
        }
    }
}

start_server {tags {"repl"}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set load_handle0 [start_write_load $primary_host $primary_port 3]
    start_server {} {
        test "Primary stream is correctly processed while the replica has a script in -BUSY state" {
            set replica [srv 0 client]
            puts [srv 0 port]
            $replica config set lua-time-limit 500
            $replica replicaof $primary_host $primary_port

            # Wait for the replica to be online
            wait_for_condition 500 100 {
                [lindex [$replica role] 3] eq {connected}
            } else {
                fail "Replica still not connected after some time"
            }

            # Wait some time to make sure the primary is sending data
            # to the replica.
            after 5000

            # Stop the ability of the replica to process data by sendig
            # a script that will put it in BUSY state.
            $replica eval {for i=1,3000000000 do end} 0

            # Wait some time again so that more primary stream will
            # be processed.
            after 2000

            # Stop the write load
            stop_write_load $load_handle0

            # number of keys
            wait_for_condition 500 100 {
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Different datasets between replica and primary"
            }
        }
    }
}
