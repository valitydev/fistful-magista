package dev.vality.fistful.magista.handler.deposit.revert;

import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.dao.DepositRevertDao;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataEventType;
import dev.vality.fistful.magista.domain.enums.DepositTransferStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositRevertData;
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
public class DepositRevertTransferCreatedHandler implements DepositEventHandler {

    private final DepositRevertDao depositRevertDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetRevert()
                && change.getChange().getRevert().getPayload().isSetTransfer()
                && change.getChange().getRevert().getPayload().getTransfer().getPayload().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Transfer transfer = change.getChange().getRevert()
                    .getPayload().getTransfer()
                    .getPayload().getCreated().getTransfer();

            long eventId = event.getEventId();
            String depositId = event.getSourceId();
            String revertId = change.getChange().getRevert().getId();
            LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            LocalDateTime eventOccuredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());
            DepositRevertDataEventType eventType = DepositRevertDataEventType.DEPOSIT_REVERT_TRANSFER_CREATED;

            log.info("Start deposit revert transfer created handling, eventId={}, depositId={}, revertId={}",
                    eventId, depositId, revertId);

            List<FinalCashFlowPosting> postings = transfer.getCashflow().getPostings();
            DepositRevertData depositRevertData = depositRevertDao.get(depositId, revertId);
            initEventFields(depositRevertData, eventId, eventCreatedAt, eventOccuredAt, eventType);
            depositRevertData.setTransferStatus(DepositTransferStatus.created);
            depositRevertData.setFee(CashFlowUtil.getFistfulFee(postings));
            depositRevertData.setProviderFee(CashFlowUtil.getFistfulProviderFee(postings));

            depositRevertDao.save(depositRevertData)
                    .ifPresentOrElse(
                            dbContractId -> log.info("Deposit revert transfer created has been saved, " +
                                    "eventId={}, depositId={}, revertId={}", eventId, depositId, revertId),
                            () -> log.info("Deposit revert transfer created has NOT been saved, " +
                                    "eventId={}, depositId={}, revertId={}", eventId, depositId, revertId));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
