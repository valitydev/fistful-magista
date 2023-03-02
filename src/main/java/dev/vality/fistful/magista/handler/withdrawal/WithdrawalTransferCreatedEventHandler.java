package dev.vality.fistful.magista.handler.withdrawal;

import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.magista.util.CashFlowUtil;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalTransferCreatedEventHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;

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
            List<FinalCashFlowPosting> postings = change
                    .getChange()
                    .getTransfer()
                    .getPayload()
                    .getCreated()
                    .getTransfer()
                    .getCashflow()
                    .getPostings();
            log.info("Trying to handle WithdrawalTransferCreated: eventId={}, withdrawalId={}", event.getEventId(),
                    event.getSourceId());

            WithdrawalData withdrawalData = getWithdrawalData(event);
            withdrawalData.setFee(CashFlowUtil.getFistfulFee(postings));
            withdrawalData.setEventId(event.getEventId());
            withdrawalData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            withdrawalData.setEventOccurredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));

            Long id = withdrawalDao.save(withdrawalData);

            log.info("WithdrawalTransferCreated has {} been saved: eventId={}, withdrawalId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private WithdrawalData getWithdrawalData(MachineEvent event) throws DaoException {
        WithdrawalData withdrawalData = withdrawalDao.get(event.getSourceId());

        if (withdrawalData == null) {
            throw new NotFoundException(
                    String.format("Withdrawal with withdrawalId='%s' not found", event.getSourceId()));
        }

        return withdrawalData;
    }

}
