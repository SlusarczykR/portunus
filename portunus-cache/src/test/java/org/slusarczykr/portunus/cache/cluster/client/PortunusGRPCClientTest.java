package org.slusarczykr.portunus.cache.cluster.client;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slusarczykr.portunus.cache.api.PortunusApiProtos.Partition;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceImplBase;
import org.slusarczykr.portunus.cache.cluster.extension.GrpcCleanupExtension;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class PortunusGRPCClientTest {

    @RegisterExtension
    static GrpcCleanupExtension cleanupExtension = new GrpcCleanupExtension();

    private PortunusGRPCClient portunusClient;
    private PortunusServiceImplBase portunusService = mock(PortunusServiceImplBase.class, delegatesTo(
            new PortunusServiceImplBase() {
                @Override
                public void getPartitions(GetPartitionsCommand request, StreamObserver<GetPartitionsDocument> responseObserver) {
                    GetPartitionsDocument document = GetPartitionsDocument.newBuilder()
                            .addAllPartitions(Collections.emptyList())
                            .build();
                    responseObserver.onNext(document);
                    responseObserver.onCompleted();
                }
            }
    ));

    @BeforeEach
    void setUp() throws IOException {
        portunusClient = new PortunusGRPCClient(cleanupExtension.addService(portunusService));
    }

    @Test
    void shouldReturnGetPartitionsDocumentWhenGetPartitions() {
        doNothing().when(portunusService).getPartitions(any(), any());

        Collection<Partition> partitions = portunusClient.getPartitions();

        assertNotNull(partitions);
        assertTrue(partitions.isEmpty());
    }
}