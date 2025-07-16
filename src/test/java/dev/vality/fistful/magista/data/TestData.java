package dev.vality.fistful.magista.data;

import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.machinegun.msgpack.Value;
import org.apache.thrift.TBase;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static dev.vality.testcontainers.annotations.util.RandomBeans.random;
import static dev.vality.testcontainers.annotations.util.RandomBeans.randomListOf;
import static dev.vality.testcontainers.annotations.util.ValuesGenerator.*;

public class TestData {

    public static final String partyId = generateString();
    public static final String walletId = generateString();

    public static SinkEvent sinkEvent(MachineEvent machineEvent) {
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);
        return sinkEvent;
    }

    public static <T extends TBase> MachineEvent machineEvent(
            ThriftSerializer<T> thriftSerializer,
            T change) {
        return new MachineEvent()
                .setEventId(1L)
                .setSourceId("source_id")
                .setSourceNs("source_ns")
                .setCreatedAt("2016-03-22T06:12:27Z")
                .setData(Value.bin(thriftSerializer.serialize("", change)));
    }

}
