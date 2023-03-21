package org.slusarczykr.portunus.cache.cluster;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

public interface Distributed<T extends Serializable> extends Serializable {

    byte[] getBytes();

    T get();

    class DistributedWrapper<T extends Serializable> implements Distributed<T> {

        private final byte[] payload;

        public DistributedWrapper(byte[] payload) {
            this.payload = payload;
        }

        public DistributedWrapper(T item) {
            this.payload = SerializationUtils.serialize(item);
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
