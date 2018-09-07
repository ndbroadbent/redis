# Check the basic monitoring and failover capabilities.

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
}

test "Instance #5 synced with the primary" {
    wait_for_condition 1000 50 {
        [RI 5 primary_link_status] eq {up}
    } else {
        fail "Instance #5 primary link status is not up"
    }
}

set current_epoch [CI 1 cluster_current_epoch]

test "Killing one primary node" {
    kill_instance redis 0
}

test "Wait for failover" {
    wait_for_condition 1000 50 {
        [CI 1 cluster_current_epoch] > $current_epoch
    } else {
        fail "No failover detected"
    }
}

test "Cluster should eventually be up again" {
    assert_cluster_state ok
}

test "Cluster is writable" {
    cluster_write_test 1
}

test "Instance #5 is now a primary" {
    assert {[RI 5 role] eq {primary}}
}

test "Restarting the previously killed primary node" {
    restart_instance redis 0
}

test "Instance #0 gets converted into a replica" {
    wait_for_condition 1000 50 {
        [RI 0 role] eq {replica}
    } else {
        fail "Old primary was not converted into replica"
    }
}
