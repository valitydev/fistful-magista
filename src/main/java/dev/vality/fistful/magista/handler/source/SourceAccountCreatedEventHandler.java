package dev.vality.fistful.magista.handler.source;

import dev.vality.fistful.account.Account;
import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.source.TimestampedChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SourceAccountCreatedEventHandler implements SourceEventHandler {

    private final SourceDao sourceDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetAccount()
                && change.getChange().getAccount().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle SourceAccountCreated: eventId={}, sourceId={}", event.getEventId(),
                    event.getSourceId());

            SourceData sourceData = getSourceData(event);
            sourceData.setEventId(event.getEventId());
            sourceData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            sourceData.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            Account account = change.getChange().getAccount().getCreated();
            sourceData.setPartyId(sourceData.getPartyId());
            sourceData.setAccountId(account.getAccountId());
            sourceData.setAccountCurrency(account.getCurrency().getSymbolicCode());
            Long id = sourceDao.save(sourceData);

            log.info("SourceAccountCreated has {} been saved: eventId={}, sourceId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private SourceData getSourceData(MachineEvent event) throws DaoException {
        SourceData sourceData = sourceDao.get(event.getSourceId());
        if (sourceData == null) {
            throw new NotFoundException(
                    String.format("Source with sourceId='%s' not found", event.getSourceId()));
        }
        return sourceData;
    }
}
