package dev.vality.fistful.magista.data;

import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.machinegun.msgpack.Value;
import org.apache.thrift.TBase;

public class TestData {

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
