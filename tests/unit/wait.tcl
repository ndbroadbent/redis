start_server {tags {"wait"}} {
start_server {} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set primary [srv -1 client]
    set primary_host [srv -1 host]
    set primary_port [srv -1 port]

    test {Setup replica} {
        $replica replicaof $primary_host $primary_port
        wait_for_condition 50 100 {
            [s 0 primary_link_status] eq {up}
        } else {
            fail "Replication not started."
        }
    }

    test {WAIT should acknowledge 1 additional copy of the data} {
        $primary set foo 0
        $primary incr foo
        $primary incr foo
        $primary incr foo
        assert {[$primary wait 1 5000] == 1}
        assert {[$replica get foo] == 3}
    }

    test {WAIT should not acknowledge 2 additional copies of the data} {
        $primary incr foo
        assert {[$primary wait 2 1000] <= 1}
    }

    test {WAIT should not acknowledge 1 additional copy if replica is blocked} {
        exec src/redis-cli -h $replica_host -p $replica_port debug sleep 5 > /dev/null 2> /dev/null &
        after 1000 ;# Give redis-cli the time to execute the command.
        $primary set foo 0
        $primary incr foo
        $primary incr foo
        $primary incr foo
        assert {[$primary wait 1 3000] == 0}
    }
}}
