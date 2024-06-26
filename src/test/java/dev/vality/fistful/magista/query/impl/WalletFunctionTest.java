package dev.vality.fistful.magista.query.impl;

import dev.vality.fistful.fistful_stat.StatRequest;
import dev.vality.fistful.fistful_stat.StatResponse;
import dev.vality.fistful.fistful_stat.StatWallet;
import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.util.TokenStringUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.magista.dsl.BadTokenException;
import dev.vality.magista.dsl.parser.QueryParserException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.*;

public class WalletFunctionTest extends AbstractIntegrationTest {

    @Autowired
    private WalletDao walletDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private WalletData walletData;
    private WalletData secondWalletData;

    @Before
    public void before() throws DaoException {
        super.before();
        walletData = random(WalletData.class);
        walletData.setId(1L);
        walletData.setWalletId("10");
        walletData.setCreatedAt(TypeUtil.stringToLocalDateTime("2022-11-24T15:28:26Z"));
        walletData.setEventCreatedAt(LocalDateTime.now().minusMinutes(1));
        walletDao.save(walletData);
        secondWalletData = random(WalletData.class);
        secondWalletData.setId(2L);
        secondWalletData.setWalletId("test1");
        secondWalletData.setCreatedAt(TypeUtil.stringToLocalDateTime("2023-11-24T15:28:26Z"));
        secondWalletData.setPartyId(walletData.getPartyId());
        secondWalletData.setIdentityId(walletData.getIdentityId());
        secondWalletData.setId(2L);
        secondWalletData.setEventCreatedAt(LocalDateTime.now());
        secondWalletData.setWalletId(secondWalletData.getWalletId());
        walletDao.save(secondWalletData);
    }

    @After
    public void after() {
        jdbcTemplate.execute("truncate mst.wallet_data");
    }

    @Test
    public void testOneWallet() throws DaoException {
        String json = String.format("{'query': {'wallets': {'party_id': '%s','identity_id': '%s', " +
                        "'currency_code':'%s', 'from_time': '%s','to_time': '%s'}}}",
                walletData.getPartyId(),
                walletData.getIdentityId(),
                walletData.getCurrencyCode(),
                TypeUtil.temporalToString(walletData.getCreatedAt().minusMinutes(1)),
                TypeUtil.temporalToString(walletData.getCreatedAt().plusMinutes(1)));
        StatResponse statResponse = queryProcessor.processQuery(new StatRequest(json));
        List<StatWallet> wallets = statResponse.getData().getWallets();
        assertEquals(1, wallets.size());
    }

    @Test
    public void testOneWalletWithWalletId() throws DaoException {
        String json = String.format("{'query': {'wallets': {'wallet_id': ['%s'], 'party_id': '%s'," +
                        "'identity_id': '%s', 'currency_code':'%s', 'from_time': '%s','to_time': '%s'}}}",
                walletData.getWalletId(),
                walletData.getPartyId(),
                walletData.getIdentityId(),
                walletData.getCurrencyCode(),
                TypeUtil.temporalToString(walletData.getCreatedAt().minusMinutes(1)),
                TypeUtil.temporalToString(walletData.getCreatedAt().plusMinutes(1)));
        StatResponse statResponse = queryProcessor.processQuery(new StatRequest(json));
        List<StatWallet> wallets = statResponse.getData().getWallets();
        assertEquals(1, wallets.size());
    }

    @Test
    public void testAllWallets() throws DaoException {
        String json = String.format("{'query': {'wallets': {'party_id': '%s','identity_id': '%s'}}}",
                walletData.getPartyId(),
                walletData.getIdentityId());
        StatResponse statResponse = queryProcessor.processQuery(new StatRequest(json));
        List<StatWallet> wallets = statResponse.getData().getWallets();
        assertEquals(2, wallets.size());
    }

    @Test(expected = QueryParserException.class)
    public void testWhenSizeOverflow() {
        String json = "{'query': {'wallets': {'size': 1001}}}";
        queryProcessor.processQuery(new StatRequest(json));
    }

    @Test
    public void testContinuationToken() {
        String json = String.format("{'query': {'wallets': {'party_id': '%s','identity_id': '%s'}, 'size':'1'}}",
                walletData.getPartyId(),
                walletData.getIdentityId());
        StatRequest statRequest = new StatRequest(json);
        StatResponse statResponse = queryProcessor.processQuery(statRequest);
        assertEquals(1, statResponse.getData().getWallets().size());
        assertNotNull(statResponse.getContinuationToken());
        assertEquals(
                "2023-11-24T15:28:26Z",
                TokenStringUtil.extractIdValue(statResponse.getContinuationToken()).get()
        );

        statRequest.setContinuationToken(statResponse.getContinuationToken());
        statResponse = queryProcessor.processQuery(statRequest);
        assertEquals(1, statResponse.getData().getWallets().size());
        assertNotNull(statResponse.getContinuationToken());
        assertEquals(
                "2022-11-24T15:28:26Z",
                TokenStringUtil.extractIdValue(statResponse.getContinuationToken()).get()
        );

        statRequest.setContinuationToken(statResponse.getContinuationToken());
        statResponse = queryProcessor.processQuery(statRequest);
        assertNull(statResponse.getContinuationToken());
    }

    @Test
    public void testIfNotPresentWallets() {
        String json = "{'query': {'wallets': {'party_id': '6954b4d1-f39f-4cc1-8843-eae834e6f849'}}}";
        StatResponse statResponse = queryProcessor.processQuery(new StatRequest(json));
        assertEquals(0, statResponse.getData().getWallets().size());
    }

    @Test(expected = BadTokenException.class)
    public void testBadToken() {
        String json = String.format("{'query': {'wallets': {'party_id': '%s','identity_id': '%s'}, 'size':'1'}}",
                walletData.getPartyId(),
                walletData.getIdentityId());
        StatRequest statRequest = new StatRequest(json);
        statRequest.setContinuationToken(UUID.randomUUID().toString());
        queryProcessor.processQuery(statRequest);
    }


}
