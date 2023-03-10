package org.slusarczykr.portunus.cache.util.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;

public class YamlResourceLoader implements ResourceLoader {
    private static final YamlResourceLoader INSTANCE = new YamlResourceLoader();

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    private YamlResourceLoader() {
    }

    public static YamlResourceLoader getInstance() {
        return INSTANCE;
    }

    @Override
    public <T> T load(String resource, Class<T> clazz) throws IOException {
        InputStream in = this.getClass().getClassLoader()
                .getResourceAsStream(resource);

        return objectMapper.readValue(in, clazz);
    }
}
