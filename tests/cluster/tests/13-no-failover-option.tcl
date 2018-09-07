# Check that the no-failover option works

source "../tests/includes/init-tests.tcl"

test "Create a 5 nodes cluster" {
    create_cluster 5 5
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Cluster is writable" {
    cluster_write_test 0
}

test "Instance #5 is a replica" {
    assert {[RI 5 role] eq {replica}}

    # Configure it to never failover the primary
    R 5 CONFIG SET cluster-replica-no-failover yes
}

test "Instance #5 synced with the primary" {
    wait_for_condition 1000 50 {
        [RI 5 primary_link_status] eq {up}
    } else {
        fail "Instance #5 primary link status is not up"
    }
}

test "The nofailover flag is propagated" {
    set replica5_id [dict get [get_myself 5] id]

    foreach_redis_id id {
        wait_for_condition 1000 50 {
            [has_flag [get_node_by_id $id $replica5_id] nofailover]
        } else {
            fail "Instance $id can't see the nofailover flag of replica"
        }
    }
}

set current_epoch [CI 1 cluster_current_epoch]

test "Killing one primary node" {
    kill_instance redis 0
}

test "Cluster should be still down after some time" {
    after 10000
    assert_cluster_state fail
}

test "Instance #5 is still a replica" {
    assert {[RI 5 role] eq {replica}}
}

test "Restarting the previously killed primary node" {
    restart_instance redis 0
}
