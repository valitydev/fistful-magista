package dev.vality.fistful.magista.handler.deposit.adjustment;

import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.dao.DepositAdjustmentDao;
import dev.vality.fistful.magista.domain.enums.DepositAdjustmentDataEventType;
import dev.vality.fistful.magista.domain.enums.DepositTransferStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositAdjustmentData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.magista.handler.deposit.DepositEventHandler;
import dev.vality.fistful.magista.util.CashFlowUtil;
import dev.vality.fistful.transfer.Transfer;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DepositAdjustmentTransferCreatedHandler implements DepositEventHandler {

    private final DepositAdjustmentDao depositAdjustmentDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetAdjustment()
                && change.getChange().getAdjustment().getPayload().isSetTransfer()
                && change.getChange().getAdjustment().getPayload().getTransfer().getPayload().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Transfer transfer = change.getChange().getAdjustment()
                    .getPayload().getTransfer()
                    .getPayload().getCreated().getTransfer();

            long eventId = event.getEventId();
            String depositId = event.getSourceId();
            String adjustmentId = change.getChange().getAdjustment().getId();
            LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            LocalDateTime eventOccuredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());
            DepositAdjustmentDataEventType eventType =
                    DepositAdjustmentDataEventType.DEPOSIT_ADJUSTMENT_TRANSFER_CREATED;

            log.info("Start deposit adjustment transfer created handling, eventId={}, depositId={}, adjustmentId={}",
                    eventId, depositId, adjustmentId);

            List<FinalCashFlowPosting> postings = transfer.getCashflow().getPostings();
            DepositAdjustmentData depositAdjustmentData = depositAdjustmentDao.get(depositId, adjustmentId);
            initEventFields(depositAdjustmentData, eventId, eventCreatedAt, eventOccuredAt, eventType);
            depositAdjustmentData.setTransferStatus(DepositTransferStatus.created);
            depositAdjustmentData.setFee(CashFlowUtil.getFistfulFee(postings));
            depositAdjustmentData.setProviderFee(CashFlowUtil.getFistfulProviderFee(postings));

            depositAdjustmentDao.save(depositAdjustmentData)
                    .ifPresentOrElse(
                            dbContractId -> log.info("Deposit adjustment transfer created has been saved, " +
                                    "eventId={}, depositId={}, adjustmentId={}", eventId, depositId, adjustmentId),
                            () -> log.info("Deposit adjustment transfer created has NOT been saved, " +
                                    "eventId={}, depositId={}, adjustmentId={}", eventId, depositId, adjustmentId));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
