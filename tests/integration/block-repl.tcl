# Test replication of blocking lists and zset operations.
# Unlike stream operations such operations are "pop" style, so they consume
# the list or sorted set, and must be replicated correctly.

proc start_bg_block_op {host port db ops} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/helpers/bg_block_op.tcl $host $port $db $ops &
}

proc stop_bg_block_op {handle} {
    catch {exec /bin/kill -9 $handle}
}

start_server {tags {"repl"}} {
    start_server {} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        set load_handle0 [start_bg_block_op $primary_host $primary_port 9 100000]
        set load_handle1 [start_bg_block_op $primary_host $primary_port 9 100000]
        set load_handle2 [start_bg_block_op $primary_host $primary_port 9 100000]

        test {First server should have role replica after REPLICAOF} {
            $replica replicaof $primary_host $primary_port
            after 1000
            s 0 role
        } {replica}

        test {Test replication with blocking lists and sorted sets operations} {
            after 25000
            stop_bg_block_op $load_handle0
            stop_bg_block_op $load_handle1
            stop_bg_block_op $load_handle2
            set retry 10
            while {$retry && ([$primary debug digest] ne [$replica debug digest])}\
            {
                after 1000
                incr retry -1
            }

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
        }
    }
}
