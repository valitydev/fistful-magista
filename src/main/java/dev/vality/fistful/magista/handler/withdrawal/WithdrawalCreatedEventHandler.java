package dev.vality.fistful.magista.handler.withdrawal;

import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.domain.enums.WithdrawalEventType;
import dev.vality.fistful.magista.domain.enums.WithdrawalStatus;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.fistful.withdrawal.Withdrawal;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalCreatedEventHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;
    private final WalletDao walletDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetCreated()
                && change.getChange().getCreated().isSetWithdrawal();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Withdrawal withdrawal = change.getChange().getCreated().getWithdrawal();
            log.info("Trying to handle WithdrawalCreated: eventId={}, withdrawalId={}", event.getEventId(),
                    event.getSourceId());

            WalletData walletData = getWalletData(withdrawal);

            WithdrawalData withdrawalData = new WithdrawalData();
            withdrawalData.setWithdrawalId(event.getSourceId());
            withdrawalData.setWalletId(withdrawal.getWalletId());
            withdrawalData.setPartyId(walletData.getPartyId());
            withdrawalData.setIdentityId(walletData.getIdentityId());
            withdrawalData.setDestinationId(withdrawal.getDestinationId());
            withdrawalData.setAmount(withdrawal.getBody().getAmount());
            withdrawalData.setCurrencyCode(withdrawal.getBody().getCurrency().getSymbolicCode());
            withdrawalData.setEventId(event.getEventId());
            withdrawalData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            LocalDateTime occurredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());
            withdrawalData.setCreatedAt(occurredAt);
            withdrawalData.setEventOccurredAt(occurredAt);
            withdrawalData.setExternalId(withdrawal.getExternalId());
            withdrawalData.setEventType(WithdrawalEventType.WITHDRAWAL_CREATED);
            withdrawalData.setWithdrawalStatus(WithdrawalStatus.pending);
            Route route = withdrawal.getRoute();
            if (Objects.nonNull(route)) {
                withdrawalData.setProviderId(route.getProviderId());
                withdrawalData.setTerminalId(route.getTerminalId());
            }

            Long id = withdrawalDao.save(withdrawalData);
            log.info("WithdrawalCreated has {} been saved: eventId={}, withdrawalId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private WalletData getWalletData(Withdrawal withdrawal) throws DaoException {
        WalletData walletData = walletDao.get(withdrawal.getWalletId());

        if (walletData == null) {
            throw new NotFoundException(
                    String.format("WalletData with walletId='%s' not found", withdrawal.getWalletId()));
        }

        if (walletData.getPartyId() == null) {
            throw new IllegalStateException(String.format("PartyId not found for WalletData with walletId='%s'; " +
                    "it must be set for correct saving of WithdrawalCreated", withdrawal.getWalletId()));
        }

        return walletData;
    }
}
