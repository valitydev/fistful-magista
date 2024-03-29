package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.dao.impl.mapper.RecordRowMapper;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.domain.tables.records.WalletDataRecord;
import dev.vality.fistful.magista.exception.DaoException;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Optional;

import static dev.vality.fistful.magista.domain.tables.WalletData.WALLET_DATA;

@Component
public class WalletDaoImpl extends AbstractGenericDao implements WalletDao {

    private final RecordRowMapper<WalletData> walletRecordRowMapper;

    @Autowired
    public WalletDaoImpl(DataSource dataSource) {
        super(dataSource);
        walletRecordRowMapper = new RecordRowMapper<>(WALLET_DATA, WalletData.class);
    }

    @Override
    public WalletData get(String walletId) throws DaoException {
        Query query = getDslContext().selectFrom(WALLET_DATA)
                .where(WALLET_DATA.WALLET_ID.eq(walletId));

        return fetchOne(query, walletRecordRowMapper);
    }

    @Override
    public Long save(WalletData wallet) throws DaoException {
        WalletDataRecord walletRecord = getDslContext().newRecord(WALLET_DATA, wallet);

        Query query = getDslContext().insertInto(WALLET_DATA)
                .set(walletRecord)
                .onConflict(WALLET_DATA.WALLET_ID)
                .doUpdate()
                .set(walletRecord)
                .where(WALLET_DATA.EVENT_ID.lessThan(wallet.getEventId()))
                .returning(WALLET_DATA.ID);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        execute(query, keyHolder);
        return keyHolder.getKey() != null ? keyHolder.getKey().longValue() : null;
    }

    @Override
    public Optional<Long> getLastEventId() throws DaoException {
        Query query = getDslContext().select(DSL.max(WALLET_DATA.EVENT_ID)).from(WALLET_DATA);
        return Optional.ofNullable(fetchOne(query, Long.class));
    }
}
