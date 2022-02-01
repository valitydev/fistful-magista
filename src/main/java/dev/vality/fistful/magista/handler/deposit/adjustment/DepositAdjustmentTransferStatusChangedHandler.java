package dev.vality.fistful.magista.handler.deposit.adjustment;

import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.dao.DepositAdjustmentDao;
import dev.vality.fistful.magista.domain.enums.DepositAdjustmentDataEventType;
import dev.vality.fistful.magista.domain.enums.DepositTransferStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositAdjustmentData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.magista.handler.deposit.DepositEventHandler;
import dev.vality.fistful.transfer.Status;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class DepositAdjustmentTransferStatusChangedHandler implements DepositEventHandler {

    private final DepositAdjustmentDao depositAdjustmentDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetAdjustment()
                && change.getChange().getAdjustment().getPayload().isSetTransfer()
                && change.getChange().getAdjustment().getPayload().getTransfer().getPayload().isSetStatusChanged();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Status status = change.getChange().getAdjustment()
                    .getPayload().getTransfer()
                    .getPayload().getStatusChanged().getStatus();

            long eventId = event.getEventId();
            String depositId = event.getSourceId();
            String adjustmentId = change.getChange().getAdjustment().getId();
            LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            LocalDateTime eventOccuredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());
            DepositAdjustmentDataEventType eventType =
                    DepositAdjustmentDataEventType.DEPOSIT_ADJUSTMENT_TRANSFER_STATUS_CHANGED;

            log.info(
                    "Start deposit adjustment transfer status changed handling, " +
                            "eventId={}, depositId={}, adjustmentId={}",
                    eventId, depositId, adjustmentId);

            DepositAdjustmentData depositAdjustmentData = depositAdjustmentDao.get(depositId, adjustmentId);
            initEventFields(depositAdjustmentData, eventId, eventCreatedAt, eventOccuredAt, eventType);
            depositAdjustmentData.setTransferStatus(TBaseUtil.unionFieldToEnum(status, DepositTransferStatus.class));

            depositAdjustmentDao.save(depositAdjustmentData)
                    .ifPresentOrElse(
                            dbContractId -> log.info("Deposit adjustment transfer status has been changed, " +
                                    "eventId={}, depositId={}, adjustmentId={}", eventId, depositId, adjustmentId),
                            () -> log.info("Deposit adjustment transfer status has NOT been changed, " +
                                    "eventId={}, depositId={}, adjustmentId={}", eventId, depositId, adjustmentId));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
