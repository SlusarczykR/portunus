package org.slusarczykr.portunus.cache.cluster;

import org.slusarczykr.portunus.cache.exception.PortunusException;

public interface Service {

    String getName();
    
    default void initialize() throws PortunusException {
    }
}
