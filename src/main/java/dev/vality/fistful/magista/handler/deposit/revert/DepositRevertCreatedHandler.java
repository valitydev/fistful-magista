package dev.vality.fistful.magista.handler.deposit.revert;

import dev.vality.fistful.base.Cash;
import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.magista.dao.DepositDao;
import dev.vality.fistful.magista.dao.DepositRevertDao;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataEventType;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.domain.tables.pojos.DepositRevertData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.magista.handler.deposit.DepositEventHandler;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class DepositRevertCreatedHandler implements DepositEventHandler {

    private final DepositRevertDao depositRevertDao;
    private final DepositDao depositDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetRevert()
                && change.getChange().getRevert().getPayload().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            var revert = change.getChange().getRevert().getPayload().getCreated().getRevert();

            long eventId = event.getEventId();
            String depositId = event.getSourceId();
            String revertId = revert.getId();
            LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
            LocalDateTime eventOccuredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());
            DepositRevertDataEventType eventType = DepositRevertDataEventType.DEPOSIT_REVERT_CREATED;
            LocalDateTime createdAt = TypeUtil.stringToLocalDateTime(revert.getCreatedAt());


            log.info("Start deposit revert created handling, eventId={}, depositId={}, revertId={}",
                    eventId, depositId, revertId);

            DepositData depositData = depositDao.get(depositId);
            DepositRevertData depositRevertData = new DepositRevertData();
            initEventFields(depositRevertData, eventId, eventCreatedAt, eventOccuredAt, eventType);
            depositRevertData.setCreatedAt(createdAt);
            depositRevertData.setSourceId(depositData.getSourceId());
            depositRevertData.setWalletId(depositData.getWalletId());
            depositRevertData.setDepositId(depositId);
            depositRevertData.setRevertId(revertId);
            Cash cash = revert.getBody();
            depositRevertData.setAmount(cash.getAmount());
            depositRevertData.setCurrencyCode(cash.getCurrency().getSymbolicCode());
            depositRevertData.setStatus(DepositRevertDataStatus.pending);
            depositRevertData.setExternalId(revert.getExternalId());
            depositRevertData.setReason(revert.getReason());
            depositRevertData.setExternalId(revert.getExternalId());
            depositRevertData.setPartyId(depositData.getPartyId());
            depositRevertData.setIdentityId(depositData.getIdentityId());
            depositRevertData.setPartyRevision(revert.getPartyRevision());
            depositRevertData.setDomainRevision(revert.getDomainRevision());

            depositRevertDao.save(depositRevertData).ifPresentOrElse(
                    dbContractId -> log
                            .info("Deposit revert created has been saved, eventId={}, depositId={}, revertId={}",
                                    eventId, depositId, revertId),
                    () -> log.info("Deposit revert created has NOT been saved, eventId={}, depositId={}, revertId={}",
                            eventId, depositId, revertId));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
