package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.Cache;
import org.slusarczykr.portunus.cache.exception.PortunusException;

public class RemotePortunusServer extends AbstractPortunusServer {

    @Override
    protected void initialize() throws PortunusException {

    }

    @Override
    public <K, V> Cache.Entry<K, V> getEntry(String key) {
        return null;
    }
}
