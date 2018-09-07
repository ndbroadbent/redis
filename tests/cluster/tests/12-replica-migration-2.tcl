# Replica migration test #2.
#
# Check that the status of primary that can be targeted by replica migration
# is acquired again, after being getting slots again, in a cluster where the
# other primaries have replicas.

source "../tests/includes/init-tests.tcl"

# Create a cluster with 5 primary and 15 replicas, to make sure there are no
# empty primaries and make rebalancing simpler to handle during the test.
test "Create a 5 nodes cluster" {
    create_cluster 5 15
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Each primary should have at least two replicas attached" {
    foreach_redis_id id {
        if {$id < 5} {
            wait_for_condition 1000 50 {
                [llength [lindex [R 0 role] 2]] >= 2
            } else {
                fail "Primary #$id does not have 2 replicas as expected"
            }
        }
    }
}

set primary0_id [dict get [get_myself 0] id]
test "Resharding all the primary #0 slots away from it" {
    set output [exec \
        ../../../src/redis-cli --cluster rebalance \
        127.0.0.1:[get_instance_attrib redis 0 port] \
        --cluster-weight ${primary0_id}=0 >@ stdout ]
}

test "Primary #0 should lose its replicas" {
    wait_for_condition 1000 50 {
        [llength [lindex [R 0 role] 2]] == 0
    } else {
        fail "Primary #0 still has replicas"
    }
}

test "Resharding back some slot to primary #0" {
    # Wait for the cluster config to propagate before attempting a
    # new resharding.
    after 10000
    set output [exec \
        ../../../src/redis-cli --cluster rebalance \
        127.0.0.1:[get_instance_attrib redis 0 port] \
        --cluster-weight ${primary0_id}=.01 \
        --cluster-use-empty-primaries  >@ stdout]
}

test "Primary #0 should re-acquire one or more replicas" {
    wait_for_condition 1000 50 {
        [llength [lindex [R 0 role] 2]] >= 1
    } else {
        fail "Primary #0 has no has replicas"
    }
}
