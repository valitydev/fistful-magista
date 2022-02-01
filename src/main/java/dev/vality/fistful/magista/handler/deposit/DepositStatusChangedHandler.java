package dev.vality.fistful.magista.handler.deposit;

import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.deposit.status.Status;
import dev.vality.fistful.magista.dao.DepositDao;
import dev.vality.fistful.magista.domain.enums.DepositEventType;
import dev.vality.fistful.magista.domain.enums.DepositStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class DepositStatusChangedHandler implements DepositEventHandler {

    private final DepositDao depositDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetStatusChanged() && change.getChange().getStatusChanged().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Status status = change.getChange().getStatusChanged().getStatus();

            long eventId = event.getEventId();
            String depositId = event.getSourceId();
            LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            LocalDateTime eventOccuredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());

            log.info("Start deposit status changed handling: eventId={}, depositId={}, status={}",
                    eventId, depositId, status);

            DepositData depositData = depositDao.get(depositId);
            initEventFields(depositData, eventId, eventCreatedAt, eventOccuredAt,
                    DepositEventType.DEPOSIT_STATUS_CHANGED);
            depositData.setDepositStatus(TBaseUtil.unionFieldToEnum(status, DepositStatus.class));

            depositDao.save(depositData);

            log.info("Deposit status has been changed: eventId={}, depositId={}, status={}",
                    eventId, depositId, status);
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
