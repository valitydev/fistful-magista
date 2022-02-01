package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.assertEquals;

public class WalletDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private WalletDao walletDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private WalletData walletData;

    @Before
    public void before() throws DaoException {
        walletData = random(WalletData.class);
        walletDao.save(walletData);
    }

    @After
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
        walletDao.save(walletDataGet);
        assertEquals(walletDao.get(this.walletData.getWalletId()).getWalletName(), modifiedWalletName);
    }
}
