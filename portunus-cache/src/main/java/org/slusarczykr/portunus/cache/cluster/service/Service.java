package org.slusarczykr.portunus.cache.cluster.service;

import org.slusarczykr.portunus.cache.exception.PortunusException;

public interface Service {

    String getName();

    boolean isInitialized();

    void initialize() throws PortunusException;
}
