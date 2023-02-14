package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.IdentityDao;
import dev.vality.fistful.magista.domain.tables.pojos.IdentityData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.*;

public class IdentityDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private IdentityDao identityDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private IdentityData identityData;

    @Before
    public void before() throws DaoException {
        identityData = random(IdentityData.class);
        identityDao.save(identityData);
    }

    @After
    public void after() {
        jdbcTemplate.execute("truncate mst.identity_data");
    }

    @Test
    public void testGetIdentityData() throws DaoException {
        IdentityData challengeDataGet = identityDao.get(this.identityData.getIdentityId());
        assertEquals(identityData.getIdentityProviderId(), challengeDataGet.getIdentityProviderId());
    }

    @Test
    public void testDuplication() throws DaoException {
        Long eventId = identityData.getEventId();
        identityData.setEventId(eventId - 1);
        assertNull(identityDao.save(identityData));
        identityData.setEventId(eventId + 1);
        assertNotNull(identityDao.save(identityData));
    }

}
