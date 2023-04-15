package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.cluster.config.ClusterConfig;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigHolder;
import org.slusarczykr.portunus.cache.cluster.config.ClusterConfigService;
import org.slusarczykr.portunus.cache.cluster.conversion.ConversionService;
import org.slusarczykr.portunus.cache.cluster.discovery.DiscoveryService;
import org.slusarczykr.portunus.cache.cluster.event.consumer.ClusterEventConsumer;
import org.slusarczykr.portunus.cache.cluster.event.publisher.ClusterEventPublisher;
import org.slusarczykr.portunus.cache.cluster.leader.election.service.LeaderElectionService;
import org.slusarczykr.portunus.cache.cluster.leader.election.starter.LeaderElectionStarterService;
import org.slusarczykr.portunus.cache.cluster.leader.vote.service.RequestVoteService;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.cluster.service.Service;
import org.slusarczykr.portunus.cache.cluster.service.ServiceManager;

public interface ClusterService extends ClusterConfigHolder, Service {

    PortunusClusterInstance getPortunusClusterInstance();

    ServiceManager getServiceManager();

    ClusterConfig getClusterConfig();

    DiscoveryService getDiscoveryService();

    PartitionService getPartitionService();

    ClusterConfigService getClusterConfigService();

    ConversionService getConversionService();

    ClusterEventPublisher getClusterEventPublisher();

    ClusterEventConsumer getClusterEventConsumer();

    LeaderElectionService getLeaderElectionService();

    RequestVoteService getRequestVoteService();

    LeaderElectionStarterService getLeaderElectionStarter();
}
