package com.rbkmoney.fistful.magista.handler.deposit.revert;

import dev.vality.fistful.deposit.TimestampedChange;
import com.rbkmoney.fistful.magista.dao.DepositRevertDao;
import com.rbkmoney.fistful.magista.domain.enums.DepositRevertDataEventType;
import com.rbkmoney.fistful.magista.domain.enums.DepositTransferStatus;
import com.rbkmoney.fistful.magista.domain.tables.pojos.DepositRevertData;
import com.rbkmoney.fistful.magista.exception.DaoException;
import com.rbkmoney.fistful.magista.exception.StorageException;
import com.rbkmoney.fistful.magista.handler.deposit.DepositEventHandler;
import dev.vality.fistful.transfer.Status;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class DepositRevertTransferStatusChangedHandler implements DepositEventHandler {

    private final DepositRevertDao depositRevertDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetRevert()
                && change.getChange().getRevert().getPayload().isSetTransfer()
                && change.getChange().getRevert().getPayload().getTransfer().getPayload().isSetStatusChanged();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Status status = change.getChange().getRevert()
                    .getPayload().getTransfer()
                    .getPayload().getStatusChanged().getStatus();

            long eventId = event.getEventId();
            String depositId = event.getSourceId();
            String revertId = change.getChange().getRevert().getId();
            LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            LocalDateTime eventOccuredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());
            DepositRevertDataEventType eventType = DepositRevertDataEventType.DEPOSIT_REVERT_TRANSFER_STATUS_CHANGED;

            log.info("Start deposit revert transfer status changed handling, eventId={}, depositId={}, revertId={}",
                    eventId, depositId, revertId);

            DepositRevertData depositRevertData = depositRevertDao.get(depositId, revertId);
            initEventFields(depositRevertData, eventId, eventCreatedAt, eventOccuredAt, eventType);
            depositRevertData.setTransferStatus(TBaseUtil.unionFieldToEnum(status, DepositTransferStatus.class));

            depositRevertDao.save(depositRevertData)
                    .ifPresentOrElse(
                            dbContractId -> log.info("Deposit revert transfer status has been changed, " +
                                    "eventId={}, depositId={}, revertId={}", eventId, depositId, revertId),
                            () -> log.info("Deposit revert transfer status has NOT been changed, " +
                                    "eventId={}, depositId={}, revertId={}", eventId, depositId, revertId));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
