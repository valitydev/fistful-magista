package dev.vality.fistful.magista.handler.withdrawal;

import dev.vality.fistful.base.Cash;
import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.withdrawal.BodyChange;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalBodyChangedEventHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetBodyChanged()
                && change.getChange().getBodyChanged().isSetNewBody();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle WithdrawalBodyChanged: eventId={}, withdrawalId={}", event.getEventId(),
                    event.getSourceId());

            WithdrawalData withdrawalData = getWithdrawalData(event);
            BodyChange bodyChanged = change.getChange().getBodyChanged();
            Cash newBody = bodyChanged.getNewBody();
            withdrawalData.setChangedAmount(newBody.getAmount());
            if (newBody.isSetCurrency()) {
                withdrawalData.setChangedCurrencyCode(newBody.getCurrency().getSymbolicCode());
            }
            withdrawalData.setEventId(event.getEventId());
            withdrawalData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            withdrawalData.setEventOccurredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));

            Long id = withdrawalDao.save(withdrawalData);

            log.info("WithdrawalBodyChanged has {} been saved: eventId={}, withdrawalId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private WithdrawalData getWithdrawalData(MachineEvent event) throws DaoException {
        WithdrawalData withdrawalData = withdrawalDao.get(event.getSourceId());

        if (withdrawalData == null) {
            throw new NotFoundException(
                    String.format("WithdrawalEvent with withdrawalId='%s' not found", event.getSourceId()));
        }

        return withdrawalData;
    }
}
