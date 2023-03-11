package org.slusarczykr.portunus.cache.cluster.server.grpc;

import io.grpc.stub.StreamObserver;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsCommand;
import org.slusarczykr.portunus.cache.api.command.PortunusCommandApiProtos.GetPartitionsDocument;
import org.slusarczykr.portunus.cache.api.service.PortunusServiceGrpc.PortunusServiceImplBase;

import java.util.Collections;

public class PortunusGRPCService extends PortunusServiceImplBase {

    @Override
    public void getPartitions(GetPartitionsCommand request, StreamObserver<GetPartitionsDocument> responseObserver) {
        responseObserver.onNext(createGetPartitionsDocument());
        responseObserver.onCompleted();
    }

    private GetPartitionsDocument createGetPartitionsDocument() {
        return GetPartitionsDocument.newBuilder()
                .addAllPartitions(Collections.emptyList())
                .build();
    }
}
