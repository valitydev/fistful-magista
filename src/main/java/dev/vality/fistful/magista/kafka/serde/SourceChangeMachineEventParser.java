package dev.vality.fistful.magista.kafka.serde;

import dev.vality.fistful.source.TimestampedChange;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import dev.vality.sink.common.serialization.BinaryDeserializer;
import org.springframework.stereotype.Service;

@Service
public class SourceChangeMachineEventParser extends MachineEventParser<TimestampedChange> {

    public SourceChangeMachineEventParser(BinaryDeserializer<TimestampedChange> deserializer) {
        super(deserializer);
    }
}