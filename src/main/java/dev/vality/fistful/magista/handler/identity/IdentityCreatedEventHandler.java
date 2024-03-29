package dev.vality.fistful.magista.handler.identity;

import dev.vality.fistful.Blocking;
import dev.vality.fistful.base.EventRange;
import dev.vality.fistful.identity.Identity;
import dev.vality.fistful.identity.IdentityState;
import dev.vality.fistful.identity.ManagementSrv;
import dev.vality.fistful.identity.TimestampedChange;
import dev.vality.fistful.magista.dao.IdentityDao;
import dev.vality.fistful.magista.domain.enums.BlockingType;
import dev.vality.fistful.magista.domain.enums.IdentityEventType;
import dev.vality.fistful.magista.domain.tables.pojos.IdentityData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.IdentityManagementClientException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdentityCreatedEventHandler implements IdentityEventHandler {

    private final IdentityDao identityDao;
    private final ManagementSrv.Iface identityManagementClient;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle IdentityCreated: eventId={}, identityId={}", event.getEventId(),
                    event.getSourceId());

            Identity identity = change.getChange().getCreated();
            LocalDateTime occurredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());

            IdentityState identityState = getIdentityState(event);
            Blocking blocking = identityState.getBlocking();
            String externalId = identity.getExternalId();

            IdentityData identityData = new IdentityData();
            identityData.setEventId(event.getEventId());
            identityData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            identityData.setCreatedAt(occurredAt);
            identityData.setEventOccurredAt(occurredAt);
            identityData.setEventType(IdentityEventType.IDENTITY_CREATED);
            identityData.setIdentityId(event.getSourceId());
            identityData.setPartyId(UUID.fromString(identity.getParty()));
            identityData.setPartyContractId(identity.getContract());
            identityData.setIdentityProviderId(identity.getProvider());
            identityData.setName(""); //todo
            identityData.setBlocking(TypeUtil.toEnumField(blocking.name(), BlockingType.class));
            identityData.setExternalId(externalId);

            Long id = identityDao.save(identityData);

            log.info("IdentityCreated has {} been saved: eventId={}, identityId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

    private IdentityState getIdentityState(MachineEvent event) {
        try {
            IdentityState identityState = identityManagementClient
                    .get(event.getSourceId(), new EventRange().setLimit((int) event.getEventId()));
            if (identityState == null) {
                throw new NotFoundException(
                        String.format("IdentityState with identityId='%s' not found", event.getSourceId()));
            }
            return identityState;
        } catch (TException e) {
            throw new IdentityManagementClientException(e);
        }
    }

}
