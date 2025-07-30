package dev.vality.fistful.magista.handler.deposit;

import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.domain.enums.DepositEventType;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.handler.EventHandler;
import dev.vality.machinegun.eventsink.MachineEvent;

import java.time.LocalDateTime;

public interface DepositEventHandler extends EventHandler<TimestampedChange, MachineEvent> {

    default void initEventFields(
            DepositData depositData,
            long eventId,
            LocalDateTime eventCreatedAt,
            LocalDateTime eventOccurredAt,
            DepositEventType eventType) {
        depositData.setId(null);
        depositData.setWtime(null);
        depositData.setEventId(eventId);
        depositData.setEventCreatedAt(eventCreatedAt);
        depositData.setEventOccurredAt(eventOccurredAt);
        depositData.setEventType(eventType);
    }
}
