package org.slusarczykr.portunus.cache.cluster;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

public interface Distributed<T extends Serializable> extends Serializable {

    ByteString getByteString();

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

        public static <T extends Serializable> Distributed<T> fromBytes(byte[] payload) {
            return new DistributedWrapper<>(payload);
        }

        @Override
        public ByteString getByteString() {
            byte[] bytes = getBytes();
            return ByteString.copyFrom(bytes);
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
