package dev.vality.fistful.magista.handler.withdrawal;

import dev.vality.fistful.magista.handler.EventHandler;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface WithdrawalEventHandler extends EventHandler<TimestampedChange, MachineEvent> {
}
