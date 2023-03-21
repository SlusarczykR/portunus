package org.slusarczykr.portunus.cache.cluster.server;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.extension.GrpcCleanupExtension;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;

import java.io.IOException;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class LocalPortunusServerTest {

    @RegisterExtension
    static GrpcCleanupExtension cleanupExtension = new GrpcCleanupExtension();

    private PortunusServiceBlockingStub portunusServiceStub;

    @BeforeEach
    void setUp() throws IOException {
        PortunusGRPCService portunusService = new PortunusGRPCService();
        portunusServiceStub = PortunusServiceGrpc.newBlockingStub(cleanupExtension.addService(portunusService));
    }

    @Test
    void shouldReturnGetPartitionsDocumentWhenGetPartitions() {
        GetPartitionsCommand command = GetPartitionsCommand.newBuilder()
                .setAddress(randomAlphabetic(12))
                .build();

        GetPartitionsDocument partitionsDocument = portunusServiceStub.getPartitions(command);

        assertNotNull(partitionsDocument);
        assertTrue(partitionsDocument.getPartitionsList().isEmpty());
    }

    @Test
    void shouldReturnContainsQueryDocumentWhenGetContainsEntry() {
        ContainsEntryQuery query = ContainsEntryQuery.newBuilder()
                .setCacheName(randomAlphabetic(12))
                .setEntryKeyType(String.class.getCanonicalName())
                .setEntryKey(ByteString.copyFrom(SerializationUtils.serialize(randomAlphabetic(12))))
                .build();

        ContainsEntryDocument containsEntryDocument = portunusServiceStub.containsEntry(query);

        assertNotNull(containsEntryDocument);
        assertFalse(containsEntryDocument.getContainsEntry());
    }
}