package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.config.PostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.DepositAdjustmentDao;
import dev.vality.fistful.magista.domain.enums.DepositAdjustmentDataEventType;
import dev.vality.fistful.magista.domain.enums.DepositAdjustmentDataStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositAdjustmentData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.jupiter.api.Assertions.*;

@PostgresqlSpringBootITest
public class DepositAdjusmentDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private DepositAdjustmentDao depositAdjustmentDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void depositAdjustmentDaoTest() throws DaoException {
        DepositAdjustmentData deposit = random(DepositAdjustmentData.class);
        deposit.setId(null);
        deposit.setStatus(DepositAdjustmentDataStatus.pending);
        deposit.setEventType(DepositAdjustmentDataEventType.DEPOSIT_ADJUSTMENT_CREATED);

        depositAdjustmentDao.save(deposit);
        deposit.setEventId(deposit.getEventId() + 1);

        Long id = depositAdjustmentDao.save(deposit).get();
        deposit.setId(id);

        assertEquals(deposit, depositAdjustmentDao.get(deposit.getDepositId(), deposit.getAdjustmentId()));

        deposit.setId(null);
        deposit.setEventType(DepositAdjustmentDataEventType.DEPOSIT_ADJUSTMENT_STATUS_CHANGED);
        deposit.setStatus(DepositAdjustmentDataStatus.succeeded);
        deposit.setEventId(deposit.getEventId() + 1);

        id = depositAdjustmentDao.save(deposit).get();
        deposit.setId(id);

        assertEquals(deposit, depositAdjustmentDao.get(deposit.getDepositId(), deposit.getAdjustmentId()));
    }


    @Test
    public void testDuplication() throws DaoException {
        DepositAdjustmentData deposit = random(DepositAdjustmentData.class);
        deposit.setId(null);
        depositAdjustmentDao.save(deposit);

        Long eventId = deposit.getEventId();
        deposit.setEventId(eventId - 1);
        assertTrue(depositAdjustmentDao.save(deposit).isEmpty());
        deposit.setEventId(eventId + 1);
        assertTrue(depositAdjustmentDao.save(deposit).isPresent());
    }

    @AfterEach
    public void after() {
        jdbcTemplate.execute("truncate mst.deposit_adjustment_data");
    }
}
