package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.config.PostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.jupiter.api.Assertions.*;

@PostgresqlSpringBootITest
public class WithdrawalDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private WithdrawalDao withdrawalDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private WithdrawalData withdrawalData;

    @BeforeEach
    public void before() throws DaoException {
        withdrawalData = random(WithdrawalData.class);
        withdrawalDao.save(withdrawalData);
    }

    @AfterEach
    public void after() {
        jdbcTemplate.execute("truncate mst.withdrawal_data");
    }

    @Test
    public void testGetWithdrawalData() throws DaoException {
        WithdrawalData withdrawalDataGet = withdrawalDao.get(this.withdrawalData.getWithdrawalId());
        assertEquals(withdrawalData.getCurrencyCode(), withdrawalDataGet.getCurrencyCode());
    }


    @Test
    public void testDuplication() throws DaoException {
        Long eventId = withdrawalData.getEventId();
        withdrawalData.setEventId(eventId - 1);
        assertNull(withdrawalDao.save(withdrawalData));
        withdrawalData.setEventId(eventId + 1);
        assertNotNull(withdrawalDao.save(withdrawalData));
    }

}
