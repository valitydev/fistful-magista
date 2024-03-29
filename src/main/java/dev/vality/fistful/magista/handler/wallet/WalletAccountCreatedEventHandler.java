package dev.vality.fistful.magista.handler.wallet;

import dev.vality.fistful.account.Account;
import dev.vality.fistful.magista.dao.IdentityDao;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.enums.WalletEventType;
import dev.vality.fistful.magista.domain.tables.pojos.IdentityData;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.wallet.TimestampedChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletAccountCreatedEventHandler implements WalletEventHandler {

    private final WalletDao walletDao;
    private final IdentityDao identityDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetAccount()
                && change.getChange().getAccount().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle WalletAccountCreated: eventId={}, walletId={}", event.getEventId(),
                    event.getSourceId());
            Account account = change.getChange().getAccount().getCreated();

            WalletData walletData = walletDao.get(event.getSourceId());
            if (walletData == null) {
                throw new NotFoundException(String.format("Wallet with walletId='%s' not found", event.getSourceId()));
            }

            IdentityData identityData = identityDao.get(account.getIdentity());
            if (identityData == null) {
                throw new NotFoundException(
                        String.format("Identity with identityId='%s' not found", account.getIdentity()));
            }

            walletData.setEventId(event.getEventId());
            walletData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            walletData.setEventOccurredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            walletData.setEventType(WalletEventType.WALLET_ACCOUNT_CREATED);
            walletData.setWalletId(event.getSourceId());
            walletData.setIdentityId(account.getIdentity());
            walletData.setCurrencyCode(account.getCurrency().getSymbolicCode());

            walletData.setPartyId(identityData.getPartyId());
            walletData.setIdentityId(identityData.getIdentityId());

            Long id = walletDao.save(walletData);
            log.info("WalletAccountCreated has {} been saved: eventId={}, walletId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }
}
