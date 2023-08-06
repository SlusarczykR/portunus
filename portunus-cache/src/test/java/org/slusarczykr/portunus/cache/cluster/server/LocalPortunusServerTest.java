package org.slusarczykr.portunus.cache.cluster.server;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.extension.GrpcCleanupExtension;
import org.slusarczykr.portunus.cache.cluster.partition.PartitionService;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;

import java.io.IOException;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalPortunusServerTest {

    @RegisterExtension
    static GrpcCleanupExtension cleanupExtension = new GrpcCleanupExtension();

    private PortunusServiceBlockingStub portunusServiceStub;

    @Mock
    private ClusterService clusterService;

    @BeforeEach
    void setUp() throws IOException {
        PortunusGRPCService portunusService = new PortunusGRPCService(clusterService);
        portunusServiceStub = PortunusServiceGrpc.newBlockingStub(cleanupExtension.addService(portunusService));
    }

    @Test
    void shouldReturnGetPartitionsDocumentWhenGetPartitions() {
        PartitionService partitionService = mock(PartitionService.class);
        when(clusterService.getPartitionService()).thenReturn(partitionService);
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
                .setEntryKey(ByteString.copyFrom(SerializationUtils.serialize(randomAlphabetic(12))))
                .build();

        ContainsEntryDocument containsEntryDocument = portunusServiceStub.containsEntry(query);

        assertNotNull(containsEntryDocument);
        assertFalse(containsEntryDocument.getContainsEntry());
    }
}