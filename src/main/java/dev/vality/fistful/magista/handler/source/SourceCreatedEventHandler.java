package dev.vality.fistful.magista.handler.source;

import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.domain.enums.SourceEventType;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.magista.util.JsonUtil;
import dev.vality.fistful.source.Source;
import dev.vality.fistful.source.TimestampedChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class SourceCreatedEventHandler implements SourceEventHandler {

    private final SourceDao sourceDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle SourceCreated: eventId={}, sourceId={}",
                    event.getEventId(), event.getSourceId());

            SourceData sourceData = new SourceData();
            sourceData.setSourceId(event.getSourceId());
            sourceData.setEventId(event.getEventId());
            sourceData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            LocalDateTime occurredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());
            sourceData.setEventOccuredAt(occurredAt);
            sourceData.setEventType(SourceEventType.SOURCE_CREATED);
            Source source = change.getChange().getCreated();
            sourceData.setName(source.getName());
            sourceData.setPartyId(UUID.fromString(source.getPartyId()));
            sourceData.setResourceInternalDetails(source.getResource().getInternal().getDetails());
            sourceData.setExternalId(source.getExternalId());
            sourceData.setCreatedAt(TypeUtil.stringToLocalDateTime(source.getCreatedAt()));
            if (source.isSetMetadata()) {
                sourceData.setContextJson(JsonUtil.objectToJsonString(source.getMetadata().entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> JsonUtil.thriftBaseToJsonNode(e.getValue())))
                ));
            }

            Long id = sourceDao.save(sourceData);
            log.info("SourceCreated has {} been saved: eventId={}, withdrawalId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }
}
