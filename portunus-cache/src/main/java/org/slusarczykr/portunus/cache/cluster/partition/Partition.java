package org.slusarczykr.portunus.cache.cluster.partition;

import lombok.Data;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class Partition {

    private final int partitionId;
    private final PortunusServer owner;
    private final Set<PortunusServer> replicaOwners;

    public Partition(int partitionId, PortunusServer owner, Collection<PortunusServer> replicaOwners) {
        this.partitionId = partitionId;
        this.owner = owner;
        this.replicaOwners = ConcurrentHashMap.newKeySet();
        this.replicaOwners.addAll(replicaOwners);
    }

    public Partition(int partitionId, PortunusServer owner) {
        this(partitionId, owner, ConcurrentHashMap.newKeySet());
    }

    public Address getOwnerAddress() {
        return owner.getAddress();
    }

    public String getOwnerPlainAddress() {
        return owner.getPlainAddress();
    }

    public void addReplicaOwner(PortunusServer replicaOwner) {
        replicaOwners.add(replicaOwner);
    }

    public void removeReplicaOwner(PortunusServer replicaOwner) {
        replicaOwners.remove(replicaOwner);
    }

    public boolean isLocal() {
        return getOwner().isLocal();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Partition partition = (Partition) o;
        return partitionId == partition.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "partitionId=" + partitionId +
                ", owner=" + owner +
                ", replicaOwners=" + Arrays.toString(replicaOwners.toArray()) +
                '}';
    }
}
