package dev.vality.fistful.magista.handler.wallet;

import dev.vality.fistful.magista.handler.EventHandler;
import dev.vality.fistful.wallet.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface WalletEventHandler extends EventHandler<TimestampedChange, MachineEvent> {
}
