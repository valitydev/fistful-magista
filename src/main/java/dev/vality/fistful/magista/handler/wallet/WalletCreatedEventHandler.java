package dev.vality.fistful.magista.handler.wallet;

import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.enums.WalletEventType;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.wallet.TimestampedChange;
import dev.vality.fistful.wallet.Wallet;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletCreatedEventHandler implements WalletEventHandler {

    private final WalletDao walletDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        log.info("Trying to handle WalletCreated: eventId={}, walletId={}", event.getEventId(), event.getSourceId());
        Wallet wallet = change.getChange().getCreated();

        WalletData walletData = new WalletData();
        walletData.setWalletId(event.getSourceId());
        walletData.setWalletName(wallet.getName());
        walletData.setEventId(event.getEventId());
        walletData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        LocalDateTime occurredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());
        walletData.setCreatedAt(occurredAt);
        walletData.setEventOccurredAt(occurredAt);
        walletData.setEventType(WalletEventType.WALLET_CREATED);
        walletData.setWalletId(event.getSourceId());

        try {
            Long id = walletDao.save(walletData);
            log.info("WalletCreated has {} been saved: eventId={}, walletId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }
}
