package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.dao.impl.mapper.RecordRowMapper;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.domain.tables.records.WithdrawalDataRecord;
import dev.vality.fistful.magista.exception.DaoException;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Optional;

import static dev.vality.fistful.magista.domain.tables.WithdrawalData.WITHDRAWAL_DATA;

@Component
public class WithdrawalDaoImpl extends AbstractGenericDao implements WithdrawalDao {

    private final RecordRowMapper<WithdrawalData> withdrawalRecordRowMapper;

    public WithdrawalDaoImpl(DataSource dataSource) {
        super(dataSource);
        withdrawalRecordRowMapper = new RecordRowMapper<>(WITHDRAWAL_DATA, WithdrawalData.class);
    }

    @Override
    public WithdrawalData get(String withdrawalId) throws DaoException {
        Query query = getDslContext()
                .selectFrom(WITHDRAWAL_DATA)
                .where(WITHDRAWAL_DATA.WITHDRAWAL_ID.eq(withdrawalId));

        return fetchOne(query, withdrawalRecordRowMapper);
    }

    @Override
    public Long save(WithdrawalData withdrawal) throws DaoException {
        WithdrawalDataRecord withdrawalRecord = getDslContext().newRecord(WITHDRAWAL_DATA, withdrawal);

        Query query = getDslContext().insertInto(WITHDRAWAL_DATA)
                .set(withdrawalRecord)
                .onConflict(WITHDRAWAL_DATA.WITHDRAWAL_ID)
                .doUpdate()
                .set(withdrawalRecord)
                .where(WITHDRAWAL_DATA.EVENT_ID.lessThan(withdrawal.getEventId()))
                .returning(WITHDRAWAL_DATA.ID);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        execute(query, keyHolder);
        return keyHolder.getKey() != null ? keyHolder.getKey().longValue() : null;
    }

    @Override
    public Optional<Long> getLastEventId() throws DaoException {
        Query query = getDslContext().select(DSL.max(WITHDRAWAL_DATA.EVENT_ID)).from(WITHDRAWAL_DATA);
        return Optional.ofNullable(fetchOne(query, Long.class));
    }
}
