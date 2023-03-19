package org.slusarczykr.portunus.cache.cluster.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

@Getter
public class ClusterConfig {

    public static final String DEFAULT_CONFIG_PATH = "portunus-config.yml";

    @JsonProperty
    private int port;

    @JsonProperty
    private List<String> members = new ArrayList<>();

    public Address getLocalServerAddress() throws PortunusException {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            return new Address(inetAddress.getHostAddress(), port);
        } catch (UnknownHostException e) {
            throw new PortunusException("Could not identify the host");
        }
    }
}
