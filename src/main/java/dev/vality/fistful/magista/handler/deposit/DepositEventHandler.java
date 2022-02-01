package dev.vality.fistful.magista.handler.deposit;

import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.domain.enums.DepositAdjustmentDataEventType;
import dev.vality.fistful.magista.domain.enums.DepositEventType;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataEventType;
import dev.vality.fistful.magista.domain.tables.pojos.DepositAdjustmentData;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.domain.tables.pojos.DepositRevertData;
import dev.vality.fistful.magista.handler.EventHandler;
import dev.vality.machinegun.eventsink.MachineEvent;

import java.time.LocalDateTime;

public interface DepositEventHandler extends EventHandler<TimestampedChange, MachineEvent> {

    default void initEventFields(
            DepositData depositData,
            long eventId,
            LocalDateTime eventCreatedAt,
            LocalDateTime eventOccuredAt,
            DepositEventType eventType) {
        depositData.setId(null);
        depositData.setWtime(null);
        depositData.setEventId(eventId);
        depositData.setEventCreatedAt(eventCreatedAt);
        depositData.setEventOccuredAt(eventOccuredAt);
        depositData.setEventType(eventType);
    }

    default void initEventFields(
            DepositAdjustmentData depositAdjustmentData,
            long eventId,
            LocalDateTime eventCreatedAt,
            LocalDateTime eventOccuredAt,
            DepositAdjustmentDataEventType eventType) {
        depositAdjustmentData.setId(null);
        depositAdjustmentData.setWtime(null);
        depositAdjustmentData.setEventId(eventId);
        depositAdjustmentData.setEventCreatedAt(eventCreatedAt);
        depositAdjustmentData.setEventOccuredAt(eventOccuredAt);
        depositAdjustmentData.setEventType(eventType);
    }

    default void initEventFields(
            DepositRevertData depositRevertData,
            long eventId,
            LocalDateTime eventCreatedAt,
            LocalDateTime eventOccuredAt,
            DepositRevertDataEventType eventType) {
        depositRevertData.setId(null);
        depositRevertData.setWtime(null);
        depositRevertData.setEventId(eventId);
        depositRevertData.setEventCreatedAt(eventCreatedAt);
        depositRevertData.setEventOccuredAt(eventOccuredAt);
        depositRevertData.setEventType(eventType);
    }
}
