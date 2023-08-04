package org.slusarczykr.portunus.cache.cluster.client;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.PartitionDTO;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryDocument;
import org.slusarczykr.portunus.cache.api.query.PortunusQueryApiProtos.ContainsEntryQuery;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceImplBase;
import org.slusarczykr.portunus.cache.cluster.ClusterService;
import org.slusarczykr.portunus.cache.cluster.extension.GrpcCleanupExtension;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class PortunusGRPCClientTest {

    @RegisterExtension
    static GrpcCleanupExtension cleanupExtension = new GrpcCleanupExtension();

    private PortunusGRPCClient portunusClient;

    @Mock
    private ClusterService clusterService;

    private final PortunusServiceImplBase portunusService = mock(PortunusServiceImplBase.class, delegatesTo(
            new PortunusServiceImplBase() {
                @Override
                public void getPartitions(GetPartitionsCommand request, StreamObserver<GetPartitionsDocument> responseObserver) {
                    GetPartitionsDocument document = GetPartitionsDocument.newBuilder()
                            .addAllPartitions(Collections.emptyList())
                            .build();
                    responseObserver.onNext(document);
                    responseObserver.onCompleted();
                }

                @Override
                public void containsEntry(ContainsEntryQuery request, StreamObserver<ContainsEntryDocument> responseObserver) {
                    ContainsEntryDocument document = ContainsEntryDocument.newBuilder()
                            .setContainsEntry(false)
                            .build();
                    responseObserver.onNext(document);
                    responseObserver.onCompleted();
                }
            }
    ));

    @BeforeEach
    void setUp() throws IOException {
        portunusClient = new PortunusGRPCClient(clusterService, cleanupExtension.addService(portunusService));
    }

    @Test
    void shouldReturnGetPartitionsDocumentWhenGetPartitions() {
        doNothing().when(portunusService).getPartitions(any(), any());

        Collection<PartitionDTO> partitions = portunusClient.getPartitions();

        assertNotNull(partitions);
        assertTrue(partitions.isEmpty());
    }

    @Test
    void shouldReturnContainsEntryDocumentWhenContainsEntry() {
        doNothing().when(portunusService).containsEntry(any(), any());

        boolean containsEntry = portunusClient.containsEntry(randomAlphabetic(12), randomAlphabetic(6));

        assertFalse(containsEntry);
    }
}