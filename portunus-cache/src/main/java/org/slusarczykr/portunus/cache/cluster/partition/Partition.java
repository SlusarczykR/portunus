package org.slusarczykr.portunus.cache.cluster.partition;

import org.slusarczykr.portunus.cache.cluster.server.PortunusServer;

public record Partition(String key, PortunusServer owner) {
}
