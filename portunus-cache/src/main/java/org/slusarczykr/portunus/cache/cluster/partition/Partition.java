package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;

public record Partition(int partitionId, PortunusServer owner) {

    public Address getOwnerAddress() {
        return owner.getAddress();
    }

    public String getOwnerPlainAddress() {
        return owner.getPlainAddress();
    }
}
