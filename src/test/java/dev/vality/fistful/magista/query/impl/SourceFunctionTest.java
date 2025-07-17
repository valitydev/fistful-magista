package dev.vality.fistful.magista.query.impl;

import dev.vality.fistful.fistful_stat.StatRequest;
import dev.vality.fistful.fistful_stat.StatResponse;
import dev.vality.fistful.fistful_stat.StatSource;
import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.config.PostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.util.TestDataGenerator;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.magista.dsl.BadTokenException;
import dev.vality.magista.dsl.TokenUtil;
import dev.vality.magista.dsl.parser.QueryParserException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@PostgresqlSpringBootITest
public class SourceFunctionTest extends AbstractIntegrationTest {

    @Autowired
    private SourceDao sourceDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private SourceData sourceData;
    private SourceData secondSourceData;

    @BeforeEach
    public void before() throws DaoException {
        super.before();
        sourceData = TestDataGenerator.create(SourceData.class);
        sourceData.setId(1L);
        sourceData.setCreatedAt(LocalDateTime.now().minusMinutes(1));
        sourceData.setSourceId(sourceData.getSourceId());
        sourceDao.save(sourceData);
        secondSourceData = TestDataGenerator.create(SourceData.class);
        secondSourceData.setId(2L);
        secondSourceData.setAccountIdentityId(sourceData.getAccountIdentityId());
        secondSourceData.setCreatedAt(LocalDateTime.now());
        sourceDao.save(secondSourceData);
    }

    @AfterEach
    public void after() {
        jdbcTemplate.execute("truncate mst.source_data");
    }

    @Test
    public void testOneSource() throws DaoException {
        String json = String.format("{'query': {'sources': {" +
                        "'source_id':'%s', " +
                        "'identity_id': '%s', " +
                        "'status':'%s', " +
                        "'currency_code':'%s', " +
                        "'from_time': '%s'," +
                        "'to_time': '%s'" +
                        "}}}",
                sourceData.getSourceId(),
                sourceData.getAccountIdentityId(),
                StringUtils.capitalize(sourceData.getStatus().getLiteral()),
                sourceData.getAccountCurrency(),
                TypeUtil.temporalToString(sourceData.getCreatedAt().minusHours(10)),
                TypeUtil.temporalToString(sourceData.getCreatedAt().plusHours(10))
        );
        StatResponse statResponse = queryProcessor.processQuery(new StatRequest(json));
        List<StatSource> sources = statResponse.getData().getSources();
        assertEquals(1, sources.size());
    }

    @Test
    public void testAllSources() throws DaoException {
        String json = String.format("{'query': {'sources': {'identity_id': '%s'}}}",
                sourceData.getAccountIdentityId());
        StatResponse statResponse = queryProcessor.processQuery(new StatRequest(json));
        List<StatSource> sources = statResponse.getData().getSources();
        assertEquals(2, sources.size());
    }

    @Test
    public void testWhenSizeOverflow() {
        String json = "{'query': {'sources': {'size': 1001}}}";
        assertThrows(QueryParserException.class, () -> {
            queryProcessor.processQuery(new StatRequest(json));
        });
    }

    @Test
    public void testContinuationToken() {
        String json = String.format("{'query': {'sources': {'identity_id': '%s'}, 'size':'1'}}",
                sourceData.getAccountIdentityId());
        StatRequest statRequest = new StatRequest(json);
        StatResponse statResponse = queryProcessor.processQuery(statRequest);
        assertEquals(1, statResponse.getData().getSources().size());
        assertNotNull(statResponse.getContinuationToken());
        assertEquals((Long) 2L, TokenUtil.extractIdValue(statResponse.getContinuationToken()).get());

        statRequest.setContinuationToken(statResponse.getContinuationToken());
        statResponse = queryProcessor.processQuery(statRequest);
        assertEquals(1, statResponse.getData().getSources().size());
        assertNotNull(statResponse.getContinuationToken());
        assertEquals((Long) 1L, TokenUtil.extractIdValue(statResponse.getContinuationToken()).get());

        statRequest.setContinuationToken(statResponse.getContinuationToken());
        statResponse = queryProcessor.processQuery(statRequest);
        assertNull(statResponse.getContinuationToken());
    }

    @Test
    public void testIfNotPresentSources() {
        String json = "{'query': {'sources': {'identity_id': '6954b4d1-f39f-4cc1-8843-eae834e6f849'}}}";
        StatResponse statResponse = queryProcessor.processQuery(new StatRequest(json));
        assertEquals(0, statResponse.getData().getSources().size());
    }

    @Test
    public void testBadToken() {
        String json = String.format("{'query': {'sources': {'identity_id': '%s'}, 'size':'1'}}",
                sourceData.getAccountIdentityId());
        StatRequest statRequest = new StatRequest(json);
        statRequest.setContinuationToken(UUID.randomUUID().toString());
        assertThrows(BadTokenException.class, () -> {
            queryProcessor.processQuery(statRequest);
        });
    }


}
