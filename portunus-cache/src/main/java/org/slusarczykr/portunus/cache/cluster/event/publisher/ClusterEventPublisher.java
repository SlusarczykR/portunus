package org.slusarczykr.portunus.cache.cluster.event.publisher;

import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.service.Service;

public interface ClusterEventPublisher extends Service {
    void publishEvent(ClusterEvent event);
}
