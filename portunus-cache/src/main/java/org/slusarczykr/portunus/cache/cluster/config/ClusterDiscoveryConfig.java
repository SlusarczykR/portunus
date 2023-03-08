package org.slusarczykr.portunus.cache.cluster.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class ClusterDiscoveryConfig {

    public static final String DEFAULT_CONFIG_PATH = "portunus-config.yml";

    @JsonProperty
    private List<String> members = new ArrayList<>();
}
