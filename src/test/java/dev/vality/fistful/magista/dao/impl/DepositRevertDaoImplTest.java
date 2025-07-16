package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.DepositRevertDao;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataEventType;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.domain.tables.pojos.DepositRevertData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.jupiter.api.Assertions.*;

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
        deposit.setEventId(deposit.getEventId() + 1);

        Long id = depositRevertDao.save(deposit).get();
        deposit.setId(id);

        assertEquals(deposit, depositRevertDao.get(deposit.getDepositId(), deposit.getRevertId()));

        deposit.setId(null);
        deposit.setEventType(DepositRevertDataEventType.DEPOSIT_REVERT_STATUS_CHANGED);
        deposit.setStatus(DepositRevertDataStatus.succeeded);

        deposit.setEventId(deposit.getEventId() + 1);
        id = depositRevertDao.save(deposit).get();
        deposit.setId(id);

        assertEquals(deposit, depositRevertDao.get(deposit.getDepositId(), deposit.getRevertId()));
    }

    @Test
    public void testDuplication() throws DaoException {
        DepositRevertData deposit = random(DepositRevertData.class);
        deposit.setId(null);
        depositRevertDao.save(deposit);
        Long eventId = deposit.getEventId();
        deposit.setEventId(eventId - 1);
        assertTrue(depositRevertDao.save(deposit).isEmpty());
        deposit.setEventId(eventId + 1);
        assertTrue(depositRevertDao.save(deposit).isPresent());
    }

    @AfterEach
    public void after() {
        jdbcTemplate.execute("truncate mst.deposit_revert_data");
    }
}
