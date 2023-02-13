package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.dao.IdentityDao;
import dev.vality.fistful.magista.domain.tables.pojos.ChallengeData;
import dev.vality.fistful.magista.exception.DaoException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.*;

public class ChallengeDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private IdentityDao identityDao;

    private ChallengeData challengeData;

    @Before
    public void before() throws DaoException {
        challengeData = random(ChallengeData.class);
        identityDao.save(challengeData);
    }


    @Test
    public void testGetChallengeData() throws DaoException {
        ChallengeData challengeDataGet = identityDao.get(
                this.challengeData.getIdentityId(),
                this.challengeData.getChallengeId());
        assertEquals(challengeData.getChallengeClassId(), challengeDataGet.getChallengeClassId());
    }

    @Test
    public void testDuplication() throws DaoException {
        Long eventId = challengeData.getEventId();
        challengeData.setEventId(eventId - 1);
        assertNull(identityDao.save(challengeData));
        challengeData.setEventId(eventId + 1);
        assertNotNull(identityDao.save(challengeData));
    }

}
