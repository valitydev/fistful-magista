package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.deposit.Change;
import dev.vality.fistful.deposit.StatusChange;
import dev.vality.fistful.deposit.TimestampedChange;
import dev.vality.fistful.deposit.status.Status;
import dev.vality.fistful.deposit.status.Succeeded;
import dev.vality.fistful.magista.config.KafkaPostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.DepositDao;
import dev.vality.fistful.magista.domain.enums.DepositStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static dev.vality.fistful.magista.data.TestData.machineEvent;
import static dev.vality.fistful.magista.data.TestData.sinkEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@KafkaPostgresqlSpringBootITest
public class DepositEventListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @Value("${kafka.topic.deposit.name}")
    private String topic;

    @MockitoBean
    private DepositDao depositDao;

    @Captor
    private ArgumentCaptor<DepositData> depositDataArgumentCaptor;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Test
    public void shouldDepositListenAndSave() throws DaoException {
        // Given
        TimestampedChange statusChanged = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.status_changed(
                        new StatusChange().setStatus(
                                Status.succeeded(new Succeeded()))));

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        statusChanged));

        when(depositDao.get("source_id"))
                .thenReturn(new DepositData());

        // When
        testThriftKafkaProducer.send(topic, sinkEvent);

        // Then
        verify(depositDao, timeout(MESSAGE_TIMEOUT).times(1))
                .save(depositDataArgumentCaptor.capture());
        assertEquals(DepositStatus.succeeded, depositDataArgumentCaptor.getValue().getDepositStatus());
    }
}
