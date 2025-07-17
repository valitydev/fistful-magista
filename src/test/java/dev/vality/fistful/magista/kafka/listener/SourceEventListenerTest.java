package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.magista.config.KafkaPostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.domain.enums.SourceStatus;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.source.*;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static dev.vality.fistful.magista.data.TestData.machineEvent;
import static dev.vality.fistful.magista.data.TestData.sinkEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@KafkaPostgresqlSpringBootITest
public class SourceEventListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @MockitoBean
    private SourceDao sourceDao;

    @Captor
    private ArgumentCaptor<SourceData> captor;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Test
    public void shouldListenAndSave() throws InterruptedException, DaoException {
        // Given
        TimestampedChange statusChanged = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.status(
                        new StatusChange().setStatus(
                                Status.authorized(new Authorized()))));

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        statusChanged));

        when(sourceDao.get("source_id"))
                .thenReturn(new SourceData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-source", sinkEvent);

        // Then
        verify(sourceDao, timeout(MESSAGE_TIMEOUT).times(1))
                .save(captor.capture());
        assertThat(captor.getValue().getStatus())
                .isEqualTo(SourceStatus.authorized);
    }
}