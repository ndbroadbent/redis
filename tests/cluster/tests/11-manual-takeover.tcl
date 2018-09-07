# Manual takeover test

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

test "Killing majority of primary nodes" {
    kill_instance redis 0
    kill_instance redis 1
    kill_instance redis 2
}

test "Cluster should eventually be down" {
    assert_cluster_state fail
}

test "Use takeover to bring replicas back" {
    R 5 cluster failover takeover
    R 6 cluster failover takeover
    R 7 cluster failover takeover
}

test "Cluster should eventually be up again" {
    assert_cluster_state ok
}

test "Cluster is writable" {
    cluster_write_test 4
}

test "Instance #5, #6, #7 are now primaries" {
    assert {[RI 5 role] eq {primary}}
    assert {[RI 6 role] eq {primary}}
    assert {[RI 7 role] eq {primary}}
}

test "Restarting the previously killed primary nodes" {
    restart_instance redis 0
    restart_instance redis 1
    restart_instance redis 2
}

test "Instance #0, #1, #2 gets converted into a replicas" {
    wait_for_condition 1000 50 {
        [RI 0 role] eq {replica} && [RI 1 role] eq {replica} && [RI 2 role] eq {replica}
    } else {
        fail "Old primaries not converted into replicas"
    }
}
