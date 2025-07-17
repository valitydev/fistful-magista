package dev.vality.fistful.magista.handler.source;

import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.domain.enums.SourceStatus;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.fistful.source.Status;
import dev.vality.fistful.source.TimestampedChange;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SourceStatusChangedEventHandler implements SourceEventHandler {

    private final SourceDao sourceDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle SourceStatusChanged: eventId={}, sourceId={}", event.getEventId(),
                    event.getSourceId());

            SourceData sourceData = getSourceData(event);
            sourceData.setEventId(event.getEventId());
            sourceData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            sourceData.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            Status status = change.getChange().getStatus().getStatus();
            sourceData.setStatus(TBaseUtil.unionFieldToEnum(status, SourceStatus.class));
            Long id = sourceDao.save(sourceData);

            log.info("SourceStatusChanged has {} been saved: eventId={}, sourceId={}",
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
