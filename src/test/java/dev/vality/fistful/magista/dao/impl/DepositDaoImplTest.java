package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.config.PostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.DepositDao;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.jupiter.api.Assertions.*;

@PostgresqlSpringBootITest
public class DepositDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private DepositDao depositDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void depositDaoTest() throws DaoException {
        DepositData deposit = random(DepositData.class);
        deposit.setId(null);
        depositDao.save(deposit);
        deposit.setEventId(deposit.getEventId() + 1);
        Long id = depositDao.save(deposit);
        deposit.setId(id);
        assertEquals(deposit, depositDao.get(deposit.getDepositId()));
    }

    @Test
    public void testDuplication() throws DaoException {
        DepositData deposit = random(DepositData.class);
        deposit.setId(null);
        depositDao.save(deposit);
        Long eventId = deposit.getEventId();
        deposit.setEventId(eventId - 1);
        assertNull(depositDao.save(deposit));
        deposit.setEventId(eventId + 1);
        assertNotNull(depositDao.save(deposit));
    }

    @AfterEach
    public void after() {
        jdbcTemplate.execute("truncate mst.deposit_data");
    }
}
