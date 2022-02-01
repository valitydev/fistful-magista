package dev.vality.fistful.magista.handler.deposit;

import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.dao.DepositDao;
import dev.vality.fistful.magista.domain.enums.DepositEventType;
import dev.vality.fistful.magista.domain.enums.DepositTransferStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.transfer.Status;
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
public class DepositTransferStatusChangedHandler implements DepositEventHandler {

    private final DepositDao depositDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetTransfer()
                && change.getChange().getTransfer().isSetPayload()
                && change.getChange().getTransfer().getPayload().isSetStatusChanged()
                && change.getChange().getTransfer().getPayload().getStatusChanged().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Status status = change.getChange().getTransfer()
                    .getPayload().getStatusChanged().getStatus();

            long eventId = event.getEventId();
            String depositId = event.getSourceId();
            LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            LocalDateTime eventOccuredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());

            log.info("Start deposit transfer status changed handling: eventId={}, depositId={}, transferChange={}",
                    eventId, depositId, change.getChange().getTransfer());

            DepositData depositData = depositDao.get(event.getSourceId());
            initEventFields(
                    depositData,
                    eventId,
                    eventCreatedAt,
                    eventOccuredAt,
                    DepositEventType.DEPOSIT_TRANSFER_STATUS_CHANGED);
            depositData.setDepositTransferStatus(TBaseUtil.unionFieldToEnum(status, DepositTransferStatus.class));

            depositDao.save(depositData);

            log.info("Deposit transfer status have been changed: eventId={}, depositId={}, transferChange={}",
                    eventId, depositId, change.getChange().getTransfer());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
