package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.DepositRevertDao;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataEventType;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositRevertData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.assertEquals;

public class DepositRevertDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private DepositRevertDao depositRevertDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void depositRevertDaoTest() throws DaoException {
        DepositRevertData deposit = random(DepositRevertData.class);
        deposit.setId(null);
        deposit.setStatus(DepositRevertDataStatus.pending);
        deposit.setEventType(DepositRevertDataEventType.DEPOSIT_REVERT_CREATED);

        depositRevertDao.save(deposit);

        Long id = depositRevertDao.save(deposit).get();
        deposit.setId(id);

        assertEquals(deposit, depositRevertDao.get(deposit.getDepositId(), deposit.getRevertId()));

        deposit.setId(null);
        deposit.setEventType(DepositRevertDataEventType.DEPOSIT_REVERT_STATUS_CHANGED);
        deposit.setStatus(DepositRevertDataStatus.succeeded);

        id = depositRevertDao.save(deposit).get();
        deposit.setId(id);

        assertEquals(deposit, depositRevertDao.get(deposit.getDepositId(), deposit.getRevertId()));
    }

    @After
    public void after() {
        jdbcTemplate.execute("truncate mst.deposit_revert_data");
    }
}
