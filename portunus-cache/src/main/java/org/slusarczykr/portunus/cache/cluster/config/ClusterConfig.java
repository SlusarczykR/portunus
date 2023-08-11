package org.slusarczykr.portunus.cache.cluster.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.slusarczykr.portunus.cache.cluster.server.PortunusServer.ClusterMemberContext.Address;
import org.slusarczykr.portunus.cache.exception.InvalidPortunusStateException;

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

    @JsonProperty
    private int port;

    @JsonProperty
    private Multicast multicast = new Multicast();

    @JsonProperty
    private List<String> members = new ArrayList<>();

    @JsonProperty
    private LeaderElection leaderElection = new LeaderElection();

    public Address getLocalServerAddress() {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            return new Address(inetAddress.getHostAddress(), port);
        } catch (UnknownHostException e) {
            throw new InvalidPortunusStateException("Could not identify the host");
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Multicast {

        public static final boolean DEFAULT_MULTICAST_ENABLED = true;
        public static final int DEFAULT_MULTICAST_PORT = 4321;

        @JsonProperty
        private boolean enabled = DEFAULT_MULTICAST_ENABLED;

        @JsonProperty
        private int port = DEFAULT_MULTICAST_PORT;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LeaderElection {

        public static final int DEFAULT_MIN_AWAIT_TIME = 3;
        public static final int DEFAULT_MAX_AWAIT_TIME = 5;
        public static final int DEFAULT_HEARTBEATS_INTERVAL = 1;
        public static final int DEFAULT_SYNC_STATE_INTERVAL = 10;

        @JsonProperty
        private int minAwaitTime = DEFAULT_MIN_AWAIT_TIME;

        @JsonProperty
        private int maxAwaitTime = DEFAULT_MAX_AWAIT_TIME;

        @JsonProperty
        private int heartbeatsInterval = DEFAULT_HEARTBEATS_INTERVAL;

        @JsonProperty
        private int syncStateInterval = DEFAULT_HEARTBEATS_INTERVAL;

        public void reset() {
            this.minAwaitTime = DEFAULT_MIN_AWAIT_TIME;
            this.maxAwaitTime = DEFAULT_MAX_AWAIT_TIME;
            this.heartbeatsInterval = DEFAULT_HEARTBEATS_INTERVAL;
            this.syncStateInterval = DEFAULT_SYNC_STATE_INTERVAL;
        }
    }
}
