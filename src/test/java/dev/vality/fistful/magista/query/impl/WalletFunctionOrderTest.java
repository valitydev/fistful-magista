package dev.vality.fistful.magista.query.impl;

import dev.vality.fistful.fistful_stat.FistfulStatisticsSrv;
import dev.vality.fistful.fistful_stat.StatRequest;
import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.geck.common.util.TypeUtil;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.jupiter.api.Assertions.*;

public class WalletFunctionOrderTest extends AbstractIntegrationTest {

    @Autowired
    private WalletDao walletDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private FistfulStatisticsSrv.Iface fistfulStatisticsHandler;

    @BeforeEach
    public void before() throws DaoException {
        super.before();
        jdbcTemplate.execute("truncate mst.wallet_data");
    }

    @AfterEach
    public void after() {
        jdbcTemplate.execute("truncate mst.wallet_data");
    }

    @Test
    public void orderTest() throws DaoException, TException {

        var createdAtList = List.of(
                "2010-10-27T01:10:59Z",
                "2011-11-27T02:10:59Z",
                "2023-10-27T03:10:59Z",
                "2023-02-27T04:10:59Z",
                "2024-12-27T05:10:59Z",
                "2024-10-27T06:10:59Z",
                "2024-07-27T07:10:59Z"
        );

        for (String createdAt : createdAtList) {
            var walletData = random(WalletData.class);
            walletData.setCreatedAt(TypeUtil.stringToLocalDateTime(createdAt));
            walletDao.save(walletData);
        }

        var result = fistfulStatisticsHandler.getWallets(
                new StatRequest("{'query':{'wallets':{'size':5}}}")
        );

        var iterator = result.getData().getWallets().iterator();
        assertEquals("2024-12-27T05:10:59Z", iterator.next().getCreatedAt());
        assertEquals("2024-10-27T06:10:59Z", iterator.next().getCreatedAt());
        assertEquals("2024-07-27T07:10:59Z", iterator.next().getCreatedAt());
        assertEquals("2023-10-27T03:10:59Z", iterator.next().getCreatedAt());
        assertEquals("2023-02-27T04:10:59Z", iterator.next().getCreatedAt());

        var secondResult = fistfulStatisticsHandler.getWallets(
                new StatRequest("{'query':{'wallets':{'size':5}}}")
                        .setContinuationToken(result.getContinuationToken())
        );

        var secondIterator = secondResult.getData().getWallets().iterator();

        assertEquals("2011-11-27T02:10:59Z", secondIterator.next().getCreatedAt());
        assertEquals("2010-10-27T01:10:59Z", secondIterator.next().getCreatedAt());
    }
}
