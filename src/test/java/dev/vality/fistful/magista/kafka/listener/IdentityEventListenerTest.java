package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.Blocking;
import dev.vality.fistful.base.EventRange;
import dev.vality.fistful.identity.*;
import dev.vality.fistful.magista.config.KafkaPostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.IdentityDao;
import dev.vality.fistful.magista.domain.tables.pojos.ChallengeData;
import dev.vality.fistful.magista.domain.tables.pojos.IdentityData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.UUID;

import static dev.vality.fistful.magista.data.TestData.machineEvent;
import static dev.vality.fistful.magista.data.TestData.sinkEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@KafkaPostgresqlSpringBootITest
public class IdentityEventListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @MockitoBean
    private IdentityDao identityDao;

    @MockitoBean
    private ManagementSrv.Iface identityManagementClient;

    @Captor
    private ArgumentCaptor<IdentityData> identityCaptor;

    @Captor
    private ArgumentCaptor<ChallengeData> challengeCaptor;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @BeforeEach
    public void setUp() throws Exception {
        IdentityState identityState = new IdentityState("name", UUID.randomUUID().toString(), "provider");
        identityState.setBlocking(Blocking.unblocked);
        identityState.setExternalId("id");
        when(identityManagementClient.get(anyString(), any(EventRange.class))).thenReturn(identityState);
    }

    @Test
    public void shouldListenAndSaveIdentityCreated() throws InterruptedException, DaoException {
        // Given
        String expected = UUID.randomUUID().toString();
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.created(new Identity(expected, "provider")));

        SinkEvent sinkEvent = sinkEvent(machineEvent(new ThriftSerializer<>(), change));

        // When
        testThriftKafkaProducer.send("mg-events-ff-identity", sinkEvent);

        // Then
        verify(identityDao, timeout(MESSAGE_TIMEOUT).times(1))
                .save(identityCaptor.capture());
        assertThat(identityCaptor.getValue().getPartyId().toString())
                .isEqualTo(expected);
    }

    @Test
    public void shouldListenAndSaveLevelChanged() throws InterruptedException, DaoException {
        // Given
        String expected = "upd";
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.level_changed(expected));

        SinkEvent sinkEvent = sinkEvent(machineEvent(new ThriftSerializer<>(), change));

        when(identityDao.get("source_id"))
                .thenReturn(new IdentityData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-identity", sinkEvent);

        // Then
        verify(identityDao, timeout(MESSAGE_TIMEOUT).times(1))
                .save(identityCaptor.capture());
        assertThat(identityCaptor.getValue().getIdentityLevelId())
                .isEqualTo(expected);
    }

    @Test
    public void shouldListenAndSaveEffectiveChallenge() throws InterruptedException, DaoException {
        // Given
        String expected = "id";
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.effective_challenge_changed(expected));

        SinkEvent sinkEvent = sinkEvent(machineEvent(new ThriftSerializer<>(), change));

        when(identityDao.get("source_id"))
                .thenReturn(new IdentityData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-identity", sinkEvent);

        // Then
        verify(identityDao, timeout(MESSAGE_TIMEOUT).times(1))
                .save(identityCaptor.capture());
        assertThat(identityCaptor.getValue().getIdentityEffectiveChallengeId())
                .isEqualTo(expected);
    }

    @Test
    public void shouldListenAndSaveChallengeCreated() throws InterruptedException, DaoException {
        // Given
        String expected = "cls";
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(getIdentityChallenge(expected));

        SinkEvent sinkEvent = sinkEvent(machineEvent(new ThriftSerializer<>(), change));

        // When
        testThriftKafkaProducer.send("mg-events-ff-identity", sinkEvent);

        // Then
        verify(identityDao, timeout(MESSAGE_TIMEOUT).times(1))
                .save(challengeCaptor.capture());
        assertThat(challengeCaptor.getValue().getChallengeClassId())
                .isEqualTo(expected);
    }

    @Test
    public void shouldListenAndSaveChallengeStatus() throws InterruptedException, DaoException {
        // Given
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.identity_challenge(new ChallengeChange("id", getApprovedStatus())));

        SinkEvent sinkEvent = sinkEvent(machineEvent(new ThriftSerializer<>(), change));

        when(identityDao.get(anyString(), anyString()))
                .thenReturn(new ChallengeData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-identity", sinkEvent);

        // Then
        verify(identityDao, timeout(MESSAGE_TIMEOUT).times(1))
                .save(challengeCaptor.capture());
        assertThat(challengeCaptor.getValue().getChallengeStatus())
                .isEqualTo(dev.vality.fistful.magista.domain.enums.ChallengeStatus.completed);
    }

    private Change getIdentityChallenge(String expected) {
        return Change.identity_challenge(
                new ChallengeChange("id", ChallengeChangePayload.created(new Challenge(expected))));
    }

    private ChallengeChangePayload getApprovedStatus() {
        return ChallengeChangePayload
                .status_changed(ChallengeStatus.completed(new ChallengeCompleted(ChallengeResolution.approved)));
    }
}
