package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.*;

public class SourceDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private SourceDao sourceDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private SourceData sourceData;

    @Before
    public void before() throws DaoException {
        sourceData = random(SourceData.class);
        sourceDao.save(sourceData);
    }

    @After
    public void after() {
        jdbcTemplate.execute("truncate mst.source_data");
    }

    @Test
    public void testGetWithdrawalData() throws DaoException {
        SourceData sourceDataGet = sourceDao.get(this.sourceData.getSourceId());
        assertEquals(this.sourceData.getAccountCurrency(), sourceDataGet.getAccountCurrency());
    }


    @Test
    public void testDuplication() throws DaoException {
        Long eventId = sourceData.getEventId();
        sourceData.setEventId(eventId - 1);
        assertNull(sourceDao.save(sourceData));
        sourceData.setEventId(eventId + 1);
        assertNotNull(sourceDao.save(sourceData));
    }

}
