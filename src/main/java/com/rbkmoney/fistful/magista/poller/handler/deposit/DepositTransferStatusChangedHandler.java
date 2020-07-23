package com.rbkmoney.fistful.magista.poller.handler.deposit;

import com.rbkmoney.fistful.deposit.TimestampedChange;
import com.rbkmoney.fistful.magista.dao.DepositDao;
import com.rbkmoney.fistful.magista.domain.enums.DepositEventType;
import com.rbkmoney.fistful.magista.domain.enums.DepositTransferStatus;
import com.rbkmoney.fistful.magista.domain.tables.pojos.DepositData;
import com.rbkmoney.fistful.magista.exception.DaoException;
import com.rbkmoney.fistful.magista.exception.StorageException;
import com.rbkmoney.fistful.transfer.Status;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

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
            Status status = change
                    .getChange()
                    .getTransfer()
                    .getPayload()
                    .getStatusChanged()
                    .getStatus();
            log.info("Start deposit transfer status changed handling: eventId={}, depositId={}, transferChange={}",
                    event.getEventId(), event.getSourceId(), change.getChange().getTransfer());

            DepositData depositData = depositDao.get(event.getSourceId());
            depositData.setEventId(event.getEventId());
            depositData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            depositData.setDepositId(event.getSourceId());
            depositData.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            depositData.setEventType(DepositEventType.DEPOSIT_TRANSFER_STATUS_CHANGED);
            depositData.setDepositTransferStatus(TBaseUtil.unionFieldToEnum(status, DepositTransferStatus.class));

            depositDao.save(depositData);
            log.info("Withdrawal deposit status have been changed: eventId={}, depositId={}, transferChange={}",
                    event.getEventId(), event.getSourceId(), change.getChange().getTransfer());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}