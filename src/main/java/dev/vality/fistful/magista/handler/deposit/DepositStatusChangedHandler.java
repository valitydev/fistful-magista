package dev.vality.fistful.magista.handler.deposit;

import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.deposit.status.Status;
import dev.vality.fistful.magista.dao.DepositDao;
import dev.vality.fistful.magista.domain.enums.DepositEventType;
import dev.vality.fistful.magista.domain.enums.DepositStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
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
            LocalDateTime eventOccurredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());

            log.info("Start deposit status changed handling: eventId={}, depositId={}, status={}",
                    eventId, depositId, status);

            DepositData depositData = depositDao.get(depositId);
            initEventFields(depositData, eventId, eventCreatedAt, eventOccurredAt,
                    DepositEventType.DEPOSIT_STATUS_CHANGED);
            depositData.setDepositStatus(resolveStatus(status));
            if (status.isSetFailed()
                    && status.getFailed().isSetFailure()
                    && status.getFailed().getFailure().isSetCode()) {
                depositData.setDepositStatusFailCode(status.getFailed().getFailure().getCode());
            }

            Long id = depositDao.save(depositData);

            log.info("Deposit status has {} been changed: eventId={}, depositId={}, status={}",
                    id == null ? "NOT" : "", eventId, depositId, status);
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private DepositStatus resolveStatus(Status status) {
        return switch (status.getSetField()) {
            case PENDING -> DepositStatus.pending;
            case SUCCEEDED -> DepositStatus.succeeded;
            case FAILED -> DepositStatus.failed;
            default -> throw new IllegalArgumentException(
                    String.format("Unknown deposit status: %s", status.getSetField()));
        };
    }
}
