package dev.vality.fistful.magista.handler.deposit;

import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.dao.DepositDao;
import dev.vality.fistful.magista.domain.enums.DepositEventType;
import dev.vality.fistful.magista.domain.enums.DepositTransferStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.magista.util.CashFlowUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class DepositTransferCreatedHandler implements DepositEventHandler {

    private final DepositDao depositDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetTransfer()
                && change.getChange().getTransfer().isSetPayload()
                && change.getChange().getTransfer().getPayload().isSetCreated()
                && change.getChange().getTransfer().getPayload().getCreated().isSetTransfer()
                && change.getChange().getTransfer().getPayload().getCreated().getTransfer().isSetCashflow();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {

            long eventId = event.getEventId();
            String depositId = event.getSourceId();
            LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            LocalDateTime eventOccuredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());

            log.info("Start deposit transfer created handling: eventId={}, depositId={}, transferChange={}",
                    eventId, depositId, change.getChange().getTransfer());

            List<FinalCashFlowPosting> postings = change.getChange().getTransfer()
                    .getPayload().getCreated().getTransfer().getCashflow().getPostings();
            DepositData depositData = depositDao.get(event.getSourceId());
            initEventFields(
                    depositData,
                    eventId,
                    eventCreatedAt,
                    eventOccuredAt,
                    DepositEventType.DEPOSIT_TRANSFER_CREATED);
            depositData.setDepositTransferStatus(DepositTransferStatus.created);
            depositData.setFee(CashFlowUtil.getFistfulFee(postings));
            depositData.setProviderFee(CashFlowUtil.getFistfulProviderFee(postings));

            depositDao.save(depositData);

            log.info("Deposit transfer created has been saved: eventId={}, depositId={}, transferChange={}",
                    eventId, depositId, change.getChange().getTransfer());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
