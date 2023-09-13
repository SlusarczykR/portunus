package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;

import java.util.Collection;
import java.util.Collections;

public class PartitionReference extends Partition {

    public PartitionReference(int partitionId) {
        super(partitionId, null, Collections.emptyList());
    }

    public PartitionReference(int partitionId, Collection<Address> replicaOwners) {
        super(partitionId, null, replicaOwners);
    }

    @Override
    public PortunusServer getOwner() {
        throw new IllegalStateException(String.format("Attempting to access owner from partition reference: %d", getPartitionId()));
    }

    @Override
    public boolean isLocal() {
        return false;
    }
}
