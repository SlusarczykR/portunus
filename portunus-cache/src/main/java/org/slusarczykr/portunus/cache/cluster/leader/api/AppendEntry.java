package org.slusarczykr.portunus.cache.cluster.leader.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
public class AppendEntry implements Serializable {

    private final long serverId;
    private final long term;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Response implements Serializable {

        private long serverId;
    }
}
