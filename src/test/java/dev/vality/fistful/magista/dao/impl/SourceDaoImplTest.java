package dev.vality.fistful.magista.dao.impl;

import dev.vality.fistful.magista.AbstractIntegrationTest;
import dev.vality.fistful.magista.config.PostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.util.TestDataGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

@PostgresqlSpringBootITest
public class SourceDaoImplTest extends AbstractIntegrationTest {

    @Autowired
    private SourceDao sourceDao;

    private SourceData sourceData;

    @BeforeEach
    public void before() throws DaoException {
        sourceData = TestDataGenerator.create(SourceData.class);
        sourceDao.save(sourceData);
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
