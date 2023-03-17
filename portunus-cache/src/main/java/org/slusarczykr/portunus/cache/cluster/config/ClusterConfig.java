package org.slusarczykr.portunus.cache.cluster.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class ClusterConfig {

    public static final String DEFAULT_CONFIG_PATH = "portunus-config.yml";

    @JsonProperty
    private int port;

    @JsonProperty
    private List<String> members = new ArrayList<>();
}
