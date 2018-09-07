# Replica migration test.
# Check that orphaned primaries are joined by replicas of primaries having
# multiple replicas attached, according to the migration barrier settings.

source "../tests/includes/init-tests.tcl"

# Create a cluster with 5 primary and 10 replicas, so that we have 2
# replicas for each primary.
test "Create a 5 nodes cluster" {
    create_cluster 5 10
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Each primary should have two replicas attached" {
    foreach_redis_id id {
        if {$id < 5} {
            wait_for_condition 1000 50 {
                [llength [lindex [R 0 role] 2]] == 2
            } else {
                fail "Primary #$id does not have 2 replicas as expected"
            }
        }
    }
}

test "Killing all the replicas of primary #0 and #1" {
    kill_instance redis 5
    kill_instance redis 10
    kill_instance redis 6
    kill_instance redis 11
    after 4000
}

foreach_redis_id id {
    if {$id < 5} {
        test "Primary #$id should have at least one replica" {
            wait_for_condition 1000 50 {
                [llength [lindex [R $id role] 2]] >= 1
            } else {
                fail "Primary #$id has no replicas"
            }
        }
    }
}

# Now test the migration to a primary which used to be a replica, after
# a failver.

source "../tests/includes/init-tests.tcl"

# Create a cluster with 5 primary and 10 replicas, so that we have 2
# replicas for each primary.
test "Create a 5 nodes cluster" {
    create_cluster 5 10
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Kill replica #7 of primary #2. Only replica left is #12 now" {
    kill_instance redis 7
}

set current_epoch [CI 1 cluster_current_epoch]

test "Killing primary node #2, #12 should failover" {
    kill_instance redis 2
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

test "Instance 12 is now a primary without replicas" {
    assert {[RI 12 role] eq {primary}}
}

# The remaining instance is now without replicas. Some other replica
# should migrate to it.

test "Primary #12 should get at least one migrated replica" {
    wait_for_condition 1000 50 {
        [llength [lindex [R 12 role] 2]] >= 1
    } else {
        fail "Primary #12 has no replicas"
    }
}
