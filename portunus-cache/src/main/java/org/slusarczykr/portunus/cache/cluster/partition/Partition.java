package org.slusarczykr.portunus.cache.cluster.partition;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class Partition {

    private static final Logger log = LoggerFactory.getLogger(Partition.class);

    private final int partitionId;
    private final PortunusServer owner;
    private final Set<Address> replicaOwners;

    public Partition(int partitionId, PortunusServer owner, Collection<Address> replicaOwners) {
        this.partitionId = partitionId;
        this.owner = owner;
        this.replicaOwners = ConcurrentHashMap.newKeySet();
        Optional.ofNullable(replicaOwners).ifPresent(this.replicaOwners::addAll);
    }

    public Partition(int partitionId, PortunusServer owner) {
        this(partitionId, owner, ConcurrentHashMap.newKeySet());
    }

    public boolean hasAnyReplicaOwner() {
        return !replicaOwners.isEmpty();
    }

    public boolean containsReplicaOwner(Address remoteServerAddress) {
        return replicaOwners.contains(remoteServerAddress);
    }

    public Address getOwnerAddress() {
        return owner.getAddress();
    }

    public String getOwnerPlainAddress() {
        return owner.getPlainAddress();
    }

    public void addReplicaOwner(Address replicaOwner) {
        log.debug("[{}] Adding replica owner: '{}'", partitionId, replicaOwner);
        replicaOwners.add(replicaOwner);
    }

    public void removeReplicaOwner(Address replicaOwner) {
        replicaOwners.remove(replicaOwner);
    }

    public boolean isLocal() {
        return getOwner().isLocal();
    }

    public boolean isReplicaOwner(Address address) {
        return getReplicaOwners().contains(address);
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

    @AllArgsConstructor
    @Getter
    public class Change<K extends Serializable, V extends Serializable> {

        private final String cacheName;
        private final Set<Cache.Entry<K, V>> entries;
        private final boolean remove;

        public Partition getPartition() {
            return Partition.this;
        }
    }
}
