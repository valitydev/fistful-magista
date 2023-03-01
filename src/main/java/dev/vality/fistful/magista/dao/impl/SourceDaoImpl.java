package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.dao.impl.mapper.RecordRowMapper;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.domain.tables.records.SourceDataRecord;
import dev.vality.fistful.magista.exception.DaoException;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Optional;

import static dev.vality.fistful.magista.domain.tables.SourceData.SOURCE_DATA;

@Component
public class SourceDaoImpl extends AbstractGenericDao implements SourceDao {

    private final RecordRowMapper<SourceData> sourceRecordRowMapper;

    public SourceDaoImpl(DataSource dataSource) {
        super(dataSource);
        sourceRecordRowMapper = new RecordRowMapper<>(SOURCE_DATA, SourceData.class);
    }

    @Override
    public SourceData get(String sourceId) throws DaoException {
        Query query = getDslContext()
                .selectFrom(SOURCE_DATA)
                .where(SOURCE_DATA.SOURCE_ID.eq(sourceId));

        return fetchOne(query, sourceRecordRowMapper);
    }

    @Override
    public Long save(SourceData source) throws DaoException {
        SourceDataRecord sourceDataRecord = getDslContext().newRecord(SOURCE_DATA, source);

        Query query = getDslContext().insertInto(SOURCE_DATA)
                .set(sourceDataRecord)
                .onConflict(SOURCE_DATA.SOURCE_ID)
                .doUpdate()
                .set(sourceDataRecord)
                .where(SOURCE_DATA.EVENT_ID.lessThan(source.getEventId()))
                .returning(SOURCE_DATA.ID);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        execute(query, keyHolder);
        return keyHolder.getKey() != null ? keyHolder.getKey().longValue() : null;
    }

    @Override
    public Optional<Long> getLastEventId() throws DaoException {
        Query query = getDslContext().select(DSL.max(SOURCE_DATA.EVENT_ID)).from(SOURCE_DATA);
        return Optional.ofNullable(fetchOne(query, Long.class));
    }
}
