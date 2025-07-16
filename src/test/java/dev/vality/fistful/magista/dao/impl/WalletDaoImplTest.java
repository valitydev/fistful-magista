package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.config.PostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.jupiter.api.Assertions.*;

@PostgresqlSpringBootITest
public class WalletDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private WalletDao walletDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private WalletData walletData;

    @BeforeEach
    public void before() throws DaoException {
        walletData = random(WalletData.class);
        walletDao.save(walletData);
    }

    @AfterEach
    public void after() {
        jdbcTemplate.execute("truncate mst.wallet_data");
    }

    @Test
    public void testGetWalletData() throws DaoException {
        WalletData walletDataGet = walletDao.get(this.walletData.getWalletId());
        assertEquals(walletData.getWalletName(), walletDataGet.getWalletName());
    }

    @Test
    public void testCorrectUpdateWalletData() throws DaoException {
        WalletData walletDataGet = walletDao.get(this.walletData.getWalletId());
        String modifiedWalletName = "kektus";
        walletDataGet.setWalletName(modifiedWalletName);
        walletDataGet.setEventId(walletDataGet.getEventId() + 1);
        walletDao.save(walletDataGet);
        assertEquals(walletDao.get(this.walletData.getWalletId()).getWalletName(), modifiedWalletName);
    }

    @Test
    public void testDuplication() throws DaoException {
        Long eventId = walletData.getEventId();
        walletData.setEventId(eventId - 1);
        assertNull(walletDao.save(walletData));
        walletData.setEventId(eventId + 1);
        assertNotNull(walletDao.save(walletData));
    }
}
