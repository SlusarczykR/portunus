package org.slusarczykr.portunus.cache.cluster.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceBlockingStub;
import org.slusarczykr.portunus.cache.cluster.server.extension.GrpcCleanupExtension;
import org.slusarczykr.portunus.cache.cluster.server.grpc.PortunusGRPCService;

import java.io.IOException;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
        assertNotNull(portunusServiceStub.getPartitions(command));
    }
}