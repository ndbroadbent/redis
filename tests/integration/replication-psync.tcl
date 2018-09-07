proc start_bg_complex_data {host port db ops} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/helpers/bg_complex_data.tcl $host $port $db $ops &
}

proc stop_bg_complex_data {handle} {
    catch {exec /bin/kill -9 $handle}
}

# Creates a primary-replica pair and breaks the link continuously to force
# partial resyncs attempts, all this while flooding the primary with
# write queries.
#
# You can specify backlog size, ttl, delay before reconnection, test duration
# in seconds, and an additional condition to verify at the end.
#
# If reconnect is > 0, the test actually try to break the connection and
# reconnect with the primary, otherwise just the initial synchronization is
# checked for consistency.
proc test_psync {descr duration backlog_size backlog_ttl delay cond diskless reconnect} {
    start_server {tags {"repl"}} {
        start_server {} {

            set primary [srv -1 client]
            set primary_host [srv -1 host]
            set primary_port [srv -1 port]
            set replica [srv 0 client]

            $primary config set repl-backlog-size $backlog_size
            $primary config set repl-backlog-ttl $backlog_ttl
            $primary config set repl-diskless-sync $diskless
            $primary config set repl-diskless-sync-delay 1

            set load_handle0 [start_bg_complex_data $primary_host $primary_port 9 100000]
            set load_handle1 [start_bg_complex_data $primary_host $primary_port 11 100000]
            set load_handle2 [start_bg_complex_data $primary_host $primary_port 12 100000]

            test {Replica should be able to synchronize with the primary} {
                $replica replicaof $primary_host $primary_port
                wait_for_condition 50 100 {
                    [lindex [r role] 0] eq {replica} &&
                    [lindex [r role] 3] eq {connected}
                } else {
                    fail "Replication not started."
                }
            }

            # Check that the background clients are actually writing.
            test {Detect write load to primary} {
                wait_for_condition 50 1000 {
                    [$primary dbsize] > 100
                } else {
                    fail "Can't detect write load from background clients."
                }
            }

            test "Test replication partial resync: $descr (diskless: $diskless, reconnect: $reconnect)" {
                # Now while the clients are writing data, break the maste-replica
                # link multiple times.
                if ($reconnect) {
                    for {set j 0} {$j < $duration*10} {incr j} {
                        after 100
                        # catch {puts "PRIMARY [$primary dbsize] keys, REPLICA [$replica dbsize] keys"}

                        if {($j % 20) == 0} {
                            catch {
                                if {$delay} {
                                    $replica multi
                                    $replica client kill $primary_host:$primary_port
                                    $replica debug sleep $delay
                                    $replica exec
                                } else {
                                    $replica client kill $primary_host:$primary_port
                                }
                            }
                        }
                    }
                }
                stop_bg_complex_data $load_handle0
                stop_bg_complex_data $load_handle1
                stop_bg_complex_data $load_handle2
                set retry 10
                while {$retry && ([$primary debug digest] ne [$replica debug digest])}\
                {
                    after 1000
                    incr retry -1
                }
                assert {[$primary dbsize] > 0}

                if {[$primary debug digest] ne [$replica debug digest]} {
                    set csv1 [csvdump r]
                    set csv2 [csvdump {r -1}]
                    set fd [open /tmp/repldump1.txt w]
                    puts -nonewline $fd $csv1
                    close $fd
                    set fd [open /tmp/repldump2.txt w]
                    puts -nonewline $fd $csv2
                    close $fd
                    puts "Primary - Replica inconsistency"
                    puts "Run diff -u against /tmp/repldump*.txt for more info"
                }
                assert_equal [r debug digest] [r -1 debug digest]
                eval $cond
            }
        }
    }
}

foreach diskless {no yes} {
    test_psync {no reconnection, just sync} 6 1000000 3600 0 {
    } $diskless 0

    test_psync {ok psync} 6 100000000 3600 0 {
        assert {[s -1 sync_partial_ok] > 0}
    } $diskless 1

    test_psync {no backlog} 6 100 3600 0.5 {
        assert {[s -1 sync_partial_err] > 0}
    } $diskless 1

    test_psync {ok after delay} 3 100000000 3600 3 {
        assert {[s -1 sync_partial_ok] > 0}
    } $diskless 1

    test_psync {backlog expired} 3 100000000 1 3 {
        assert {[s -1 sync_partial_err] > 0}
    } $diskless 1
}
