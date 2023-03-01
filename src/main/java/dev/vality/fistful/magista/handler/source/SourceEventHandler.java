package dev.vality.fistful.magista.handler.source;

import dev.vality.fistful.source.TimestampedChange;
import dev.vality.fistful.magista.handler.EventHandler;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface SourceEventHandler extends EventHandler<TimestampedChange, MachineEvent> {
}
