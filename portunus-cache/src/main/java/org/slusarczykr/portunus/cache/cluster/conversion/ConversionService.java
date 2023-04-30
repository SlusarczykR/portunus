package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.AddressDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.CacheEntryDTO;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.VirtualPortunusNodeDTO;
import org.slusarczykr.portunus.cache.cluster.leader.api.RequestVote;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;
import org.slusarczykr.portunus.cache.cluster.partition.circle.PortunusConsistentHashingCircle.VirtualPortunusNode;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.AppendEntry;
import org.slusarczykr.portunus.cache.paxos.api.PortunusPaxosApiProtos.RequestVoteResponse;

import java.io.Serializable;

public interface ConversionService extends Service {

    Partition convert(PortunusApiProtos.PartitionDTO partition);

    PortunusApiProtos.PartitionDTO convert(Partition partition);

    <K extends Serializable, V extends Serializable> Cache.Entry<K, V> convert(CacheEntryDTO cacheEntry);

    <K extends Serializable, V extends Serializable> CacheEntryDTO convert(Cache.Entry<K, V> cacheEntry);

    Address convert(AddressDTO address);

    AddressDTO convert(Address address);

    RequestVote.Response convert(RequestVoteResponse requestVoteResponse);

    RequestVoteResponse convert(RequestVote.Response requestVoteResponse);

    RequestVote convert(AppendEntry appendEntry);

    VirtualPortunusNode convert(VirtualPortunusNodeDTO virtualPortunusNode);
}
