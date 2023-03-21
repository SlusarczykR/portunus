package org.slusarczykr.portunus.cache.cluster.conversion;

import org.slusarczykr.portunus.cache.api.PortunusApiProtos;
import org.slusarczykr.portunus.cache.cluster.partition.Partition;

public interface ConversionService {

    Partition convert(PortunusApiProtos.Partition partition);

    PortunusApiProtos.Partition convert(Partition partition);
}
