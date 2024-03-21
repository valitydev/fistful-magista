package dev.vality.fistful.magista.query.impl;

import dev.vality.fistful.fistful_stat.FistfulStatisticsSrv;
import dev.vality.fistful.fistful_stat.StatRequest;
import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.*;

public class WalletFunctionOrderTest extends AbstractIntegrationTest {

    @Autowired
    private WalletDao walletDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private FistfulStatisticsSrv.Iface fistfulStatisticsHandler;

    @Before
    public void before() throws DaoException {
        super.before();
        jdbcTemplate.execute("truncate mst.wallet_data");
    }

    @Test
    public void orderTest() throws DaoException, TException {
        WalletData walletData1 = random(WalletData.class);
        walletData1.setId(1L);
        walletData1.setWalletId("100");

        WalletData walletData2 = random(WalletData.class);
        walletData2.setId(2L);
        walletData2.setWalletId("1");

        WalletData walletData3 = random(WalletData.class);
        walletData3.setId(3L);
        walletData3.setWalletId("10");

        walletDao.save(walletData1);
        walletDao.save(walletData2);
        walletDao.save(walletData3);

        var result = fistfulStatisticsHandler.getWallets(
                new StatRequest("{\"query\":{\"wallets\":{\"size\":25}}}")
        );

        var iterator = result.getData().getWallets().iterator();

        assertEquals("100", iterator.next().getId());
        assertEquals("10", iterator.next().getId());
        assertEquals("1", iterator.next().getId());
    }


}
