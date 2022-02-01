package dev.vality.fistful.magista.handler.identity;

import dev.vality.fistful.identity.TimestampedChange;
import dev.vality.fistful.magista.handler.EventHandler;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface IdentityEventHandler extends EventHandler<TimestampedChange, MachineEvent> {
}
