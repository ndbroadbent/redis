start_server {tags {"psync2"}} {
start_server {} {
start_server {} {
start_server {} {
start_server {} {
    set primary_id 0                 ; # Current primary
    set start_time [clock seconds]  ; # Test start time
    set counter_value 0             ; # Current value of the Redis counter "x"

    # Config
    set debug_msg 0                 ; # Enable additional debug messages

    set no_exit 0                   ; # Do not exit at end of the test

    set duration 20                 ; # Total test seconds

    set genload 1                   ; # Load primary with writes at every cycle

    set genload_time 5000           ; # Writes duration time in ms

    set disconnect 1                ; # Break replication link between random
                                      # primary and replica instances while the
                                      # primary is loaded with writes.

    set disconnect_period 1000      ; # Disconnect repl link every N ms.

    for {set j 0} {$j < 5} {incr j} {
        set R($j) [srv [expr 0-$j] client]
        set R_host($j) [srv [expr 0-$j] host]
        set R_port($j) [srv [expr 0-$j] port]
        if {$debug_msg} {puts "Log file: [srv [expr 0-$j] stdout]"}
    }

    set cycle 1
    while {([clock seconds]-$start_time) < $duration} {
        test "PSYNC2: --- CYCLE $cycle ---" {}
        incr cycle

        # Create a random replication layout.
        # Start with switching primary (this simulates a failover).

        # 1) Select the new primary.
        set primary_id [randomInt 5]
        set used [list $primary_id]
        test "PSYNC2: \[NEW LAYOUT\] Set #$primary_id as primary" {
            $R($primary_id) replicaof no one
            if {$counter_value == 0} {
                $R($primary_id) set x $counter_value
            }
        }

        # 2) Attach all the replicas to a random instance
        while {[llength $used] != 5} {
            while 1 {
                set replica_id [randomInt 5]
                if {[lsearch -exact $used $replica_id] == -1} break
            }
            set rand [randomInt [llength $used]]
            set mid [lindex $used $rand]
            set primary_host $R_host($mid)
            set primary_port $R_port($mid)

            test "PSYNC2: Set #$replica_id to replicate from #$mid" {
                $R($replica_id) replicaof $primary_host $primary_port
            }
            lappend used $replica_id
        }

        # 3) Increment the counter and wait for all the instances
        # to converge.
        test "PSYNC2: cluster is consistent after failover" {
            $R($primary_id) incr x; incr counter_value
            for {set j 0} {$j < 5} {incr j} {
                wait_for_condition 50 1000 {
                    [$R($j) get x] == $counter_value
                } else {
                    fail "Instance #$j x variable is inconsistent"
                }
            }
        }

        # 4) Generate load while breaking the connection of random
        # replica-primary pairs.
        test "PSYNC2: generate load while killing replication links" {
            set t [clock milliseconds]
            set next_break [expr {$t+$disconnect_period}]
            while {[clock milliseconds]-$t < $genload_time} {
                if {$genload} {
                    $R($primary_id) incr x; incr counter_value
                }
                if {[clock milliseconds] == $next_break} {
                    set next_break \
                        [expr {[clock milliseconds]+$disconnect_period}]
                    set replica_id [randomInt 5]
                    if {$disconnect} {
                        $R($replica_id) client kill type primary
                        if {$debug_msg} {
                            puts "+++ Breaking link for replica #$replica_id"
                        }
                    }
                }
            }
        }

        # 5) Increment the counter and wait for all the instances
        set x [$R($primary_id) get x]
        test "PSYNC2: cluster is consistent after load (x = $x)" {
            for {set j 0} {$j < 5} {incr j} {
                wait_for_condition 50 1000 {
                    [$R($j) get x] == $counter_value
                } else {
                    fail "Instance #$j x variable is inconsistent"
                }
            }
        }

        # Put down the old primary so that it cannot generate more
        # replication stream, this way in the next primary switch, the time at
        # which we move replicas away is not important, each will have full
        # history (otherwise PINGs will make certain replicas have more history),
        # and sometimes a full resync will be needed.
        $R($primary_id) replicaof 127.0.0.1 0 ;# We use port zero to make it fail.

        if {$debug_msg} {
            for {set j 0} {$j < 5} {incr j} {
                puts "$j: sync_full: [status $R($j) sync_full]"
                puts "$j: id1      : [status $R($j) primary_replid]:[status $R($j) primary_repl_offset]"
                puts "$j: id2      : [status $R($j) primary_replid2]:[status $R($j) second_repl_offset]"
                puts "$j: backlog  : firstbyte=[status $R($j) repl_backlog_first_byte_offset] len=[status $R($j) repl_backlog_histlen]"
                puts "---"
            }
        }

        test "PSYNC2: total sum of full synchronizations is exactly 4" {
            set sum 0
            for {set j 0} {$j < 5} {incr j} {
                incr sum [status $R($j) sync_full]
            }
            assert {$sum == 4}
        }

        # Limit anyway the maximum number of cycles. This is useful when the
        # test is skipped via --only option of the test suite. In that case
        # we don't want to see many seconds of this test being just skipped.
        if {$cycle > 50} break
    }

    test "PSYNC2: Bring the primary back again for next test" {
        $R($primary_id) replicaof no one
        set primary_host $R_host($primary_id)
        set primary_port $R_port($primary_id)
        for {set j 0} {$j < 5} {incr j} {
            if {$j == $primary_id} continue
            $R($j) replicaof $primary_host $primary_port
        }

        # Wait for replicas to sync
        wait_for_condition 50 1000 {
            [status $R($primary_id) connected_replicas] == 4
        } else {
            fail "Replica not reconnecting"
        }
    }

    test "PSYNC2: Partial resync after restart using RDB aux fields" {
        # Pick a random replica
        set replica_id [expr {($primary_id+1)%5}]
        set sync_count [status $R($primary_id) sync_full]
        catch {
            $R($replica_id) config rewrite
            $R($replica_id) debug restart
        }
        wait_for_condition 50 1000 {
            [status $R($primary_id) connected_replicas] == 4
        } else {
            fail "Replica not reconnecting"
        }
        set new_sync_count [status $R($primary_id) sync_full]
        assert {$sync_count == $new_sync_count}
    }

    test "PSYNC2: Replica RDB restart with EVALSHA in backlog issue #4483" {
        # Pick a random replica
        set replica_id [expr {($primary_id+1)%5}]
        set sync_count [status $R($primary_id) sync_full]

        # Make sure to replicate the first EVAL while the salve is online
        # so that it's part of the scripts the primary believes it's safe
        # to propagate as EVALSHA.
        $R($primary_id) EVAL {return redis.call("incr","__mycounter")} 0
        $R($primary_id) EVALSHA e6e0b547500efcec21eddb619ac3724081afee89 0

        # Wait for the two to sync
        wait_for_condition 50 1000 {
            [$R($primary_id) debug digest] == [$R($replica_id) debug digest]
        } else {
            fail "Replica not reconnecting"
        }

        # Prevent the replica from receiving primary updates, and at
        # the same time send a new script several times to the
        # primary, so that we'll end with EVALSHA into the backlog.
        $R($replica_id) replicaof 127.0.0.1 0

        $R($primary_id) EVALSHA e6e0b547500efcec21eddb619ac3724081afee89 0
        $R($primary_id) EVALSHA e6e0b547500efcec21eddb619ac3724081afee89 0
        $R($primary_id) EVALSHA e6e0b547500efcec21eddb619ac3724081afee89 0

        catch {
            $R($replica_id) config rewrite
            $R($replica_id) debug restart
        }

        # Reconfigure the replica correctly again, when it's back online.
        set retry 50
        while {$retry} {
            if {[catch {
                $R($replica_id) replicaof $primary_host $primary_port
            }]} {
                after 1000
            } else {
                break
            }
            incr retry -1
        }

        # The primary should be back at 4 replicas eventually
        wait_for_condition 50 1000 {
            [status $R($primary_id) connected_replicas] == 4
        } else {
            fail "Replica not reconnecting"
        }
        set new_sync_count [status $R($primary_id) sync_full]
        assert {$sync_count == $new_sync_count}

        # However if the replica started with the full state of the
        # scripting engine, we should now have the same digest.
        wait_for_condition 50 1000 {
            [$R($primary_id) debug digest] == [$R($replica_id) debug digest]
        } else {
            fail "Debug digest mismatch between primary and replica in post-restart handshake"
        }
    }

    if {$no_exit} {
        while 1 { puts -nonewline .; flush stdout; after 1000}
    }

}}}}}
