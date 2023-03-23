package org.slusarczykr.portunus.cache.cluster.server;

import org.slusarczykr.portunus.cache.cluster.conversion.ConversionService;
import org.slusarczykr.portunus.cache.cluster.conversion.DefaultConversionService;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.FatalPortunusException;
import org.slusarczykr.portunus.cache.exception.PortunusException;

public abstract class AbstractPortunusServer implements PortunusServer {

    protected final ClusterMemberContext serverContext;
    protected final ConversionService conversionService;

    protected AbstractPortunusServer(ClusterMemberContext serverContext) {
        try {
            this.serverContext = serverContext;
            this.conversionService = DefaultConversionService.getInstance();
            initialize();
        } catch (Exception e) {
            throw new FatalPortunusException("Portunus server initialization failed", e);
        }
    }

    protected abstract void initialize() throws PortunusException;

    @Override
    public String getPlainAddress() {
        return serverContext.getPlainAddress();
    }

    @Override
    public Address getAddress() {
        return serverContext.address();
    }
}
