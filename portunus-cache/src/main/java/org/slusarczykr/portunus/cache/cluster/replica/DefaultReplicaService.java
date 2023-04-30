package org.slusarczykr.portunus.cache.cluster.replica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.service.AbstractService;

public class DefaultReplicaService extends AbstractService implements ReplicaService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReplicaService.class);

    public static DefaultReplicaService newInstance(ClusterService clusterService) {
        return new DefaultReplicaService(clusterService);
    }

    private DefaultReplicaService(ClusterService clusterService) {
        super(clusterService);
    }

    @Override
    public String getName() {
        return ReplicaService.class.getSimpleName();
    }
}
