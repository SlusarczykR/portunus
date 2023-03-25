package org.slusarczykr.portunus.cache.cluster.event.consumer;

import org.slusarczykr.portunus.cache.api.event.PortunusEventApiProtos.ClusterEvent;
import org.slusarczykr.portunus.cache.cluster.Service;

public interface ClusterEventConsumer extends Service {

    void consumeEvent(ClusterEvent event);
}
