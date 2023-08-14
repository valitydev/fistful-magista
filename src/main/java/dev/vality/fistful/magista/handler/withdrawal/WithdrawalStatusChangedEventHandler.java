package dev.vality.fistful.magista.handler.withdrawal;

import dev.vality.fistful.base.Failure;
import dev.vality.fistful.base.SubFailure;
import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.domain.enums.WithdrawalEventType;
import dev.vality.fistful.magista.domain.enums.WithdrawalStatus;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalStatusChangedEventHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetStatusChanged()
                && change.getChange().getStatusChanged().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle WithdrawalStatusChanged: eventId={}, withdrawalId={}", event.getEventId(),
                    event.getSourceId());

            WithdrawalData withdrawalData = getWithdrawalData(event);
            withdrawalData.setEventId(event.getEventId());
            withdrawalData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            withdrawalData.setEventOccurredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            withdrawalData.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
            Status status = change.getChange().getStatusChanged().getStatus();
            withdrawalData.setWithdrawalStatus(TBaseUtil.unionFieldToEnum(status, WithdrawalStatus.class));
            if (status.isSetFailed() && status.getFailed().isSetFailure()) {
                Failure failure = status.getFailed().getFailure();
                withdrawalData.setErrorCode(failure.getCode());
                withdrawalData.setErrorReason(failure.getReason());
                SubFailure subFailure = failure.getSub();
                if (subFailure != null && subFailure.isSetCode()) {
                    withdrawalData.setErrorSubFailure(subFailure.getCode());
                }
            }

            Long id = withdrawalDao.save(withdrawalData);

            log.info("WithdrawalStatusChanged has {} been saved: eventId={}, withdrawalId={}",
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
