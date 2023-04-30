package org.slusarczykr.portunus.cache.cluster.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.PortunusException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterConfig {

    public static final String DEFAULT_CONFIG_PATH = "portunus-config.yml";
    public static final boolean DEFAULT_MULTICAST_ENABLED = true;
    public static final int DEFAULT_MULTICAST_PORT = 4321;

    @JsonProperty
    private int port;

    @JsonProperty
    private boolean multicast = true;

    @JsonProperty
    private int multicastPort = 4321;

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
