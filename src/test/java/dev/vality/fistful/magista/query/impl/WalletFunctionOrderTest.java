package dev.vality.fistful.magista.query.impl;

import dev.vality.fistful.fistful_stat.FistfulStatisticsSrv;
import dev.vality.fistful.fistful_stat.StatRequest;
import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

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

    @After
    public void after() {
        jdbcTemplate.execute("truncate mst.wallet_data");
    }

    @Test
    public void orderTest() throws DaoException, TException {

        var ids = List.of("test", "test02", "test01", "01", "2", "10", "100");

        for (String id : ids) {
            var walletData = random(WalletData.class);
            walletData.setWalletId(id);
            walletDao.save(walletData);
        }

        var result = fistfulStatisticsHandler.getWallets(
                new StatRequest("{\"query\":{\"wallets\":{\"size\":25}}}")
        );

        var iterator = result.getData().getWallets().iterator();
        assertEquals("01", iterator.next().getId());
        assertEquals("2", iterator.next().getId());
        assertEquals("10", iterator.next().getId());
        assertEquals("100", iterator.next().getId());
        assertEquals("test", iterator.next().getId());
        assertEquals("test01", iterator.next().getId());
        assertEquals("test02", iterator.next().getId());
    }
}
