package dev.vality.fistful.magista.service;

import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.handler.deposit.DepositEventHandler;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class DepositEventService {

    private final List<DepositEventHandler> depositEventHandlers;
    private final MachineEventParser<TimestampedChange> parser;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.forEach(this::handleIfAccept);
    }

    private void handleIfAccept(MachineEvent machineEvent) {
        TimestampedChange change = parser.parse(machineEvent);

        if (change.isSetChange()) {
            depositEventHandlers.stream()
                    .filter(handler -> handler.accept(change))
                    .forEach(handler -> handler.handle(change, machineEvent));
        }
    }
}
