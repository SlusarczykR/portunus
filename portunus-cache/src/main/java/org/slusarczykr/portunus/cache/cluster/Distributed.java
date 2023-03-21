package org.slusarczykr.portunus.cache.cluster;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

public interface Distributed<T extends Serializable> extends Serializable {

    byte[] getBytes();

    T get();

    class DistributedWrapper<T extends Serializable> implements Distributed<T> {

        private final byte[] payload;

        private DistributedWrapper(byte[] payload) {
            this.payload = payload;
        }

        private DistributedWrapper(T item) {
            this.payload = SerializationUtils.serialize(item);
        }

        public static <T extends Serializable> Distributed<T> from(T item) {
            return new DistributedWrapper<>(item);
        }

        public static <T extends Serializable> Distributed<T> fromBytes(byte[] payload, Class<T> clazz) {
            return new DistributedWrapper<>(payload);
        }

        @Override
        public byte[] getBytes() {
            return payload;
        }

        @Override
        public T get() {
            return SerializationUtils.deserialize(payload);
        }
    }
}
