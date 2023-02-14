package dev.vality.fistful.magista.handler.identity;

import dev.vality.fistful.identity.TimestampedChange;
import dev.vality.fistful.magista.dao.IdentityDao;
import dev.vality.fistful.magista.domain.enums.IdentityEventType;
import dev.vality.fistful.magista.domain.tables.pojos.IdentityData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdentityLevelChangedEventHandler implements IdentityEventHandler {

    private final IdentityDao identityDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetLevelChanged();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle IdentityLevelChanged: eventId={}, identityId={}", event.getEventId(),
                    event.getSourceId());

            IdentityData identityData = getIdentityData(event);
            identityData.setEventId(event.getEventId());
            identityData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            identityData.setEventOccurredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            identityData.setEventType(IdentityEventType.IDENTITY_LEVEL_CHANGED);
            identityData.setIdentityLevelId(change.getChange().getLevelChanged());

            Long id = identityDao.save(identityData);

            log.info("IdentityLevelChanged has {} been saved, eventId={}, identityId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private IdentityData getIdentityData(MachineEvent event) throws DaoException {
        IdentityData identityData = identityDao.get(event.getSourceId());
        if (identityData == null) {
            throw new NotFoundException(String.format("Identity with id='%s' not found", event.getSourceId()));
        }
        return identityData;
    }
}
