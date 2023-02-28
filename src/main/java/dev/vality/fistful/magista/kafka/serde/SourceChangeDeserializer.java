package dev.vality.fistful.magista.kafka.serde;

import dev.vality.fistful.source.TimestampedChange;
import dev.vality.sink.common.serialization.impl.AbstractThriftBinaryDeserializer;
import org.springframework.stereotype.Service;

@Service
public class SourceChangeDeserializer extends AbstractThriftBinaryDeserializer<TimestampedChange> {

    @Override
    public TimestampedChange deserialize(byte[] bin) {
        return deserialize(bin, new TimestampedChange());
    }
}
