package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.magista.FistfulMagistaApplication;
import dev.vality.fistful.magista.config.KafkaPostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.RouteChange;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static dev.vality.fistful.magista.data.TestData.machineEvent;
import static dev.vality.fistful.magista.data.TestData.sinkEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@KafkaPostgresqlSpringBootITest
public class WithdrawalRouteEventListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @MockBean
    private WithdrawalDao withdrawalDao;

    @Captor
    private ArgumentCaptor<WithdrawalData> captor;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Test
    public void shouldListenAndSave() throws InterruptedException, DaoException {
        // Given
        int providerId = 1;
        TimestampedChange statusChanged = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.route(
                        new RouteChange()
                                .setRoute(new Route()
                                        .setProviderId(providerId)
                                        .setTerminalId(2))));

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        statusChanged));

        when(withdrawalDao.get("source_id"))
                .thenReturn(new WithdrawalData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-withdrawal", sinkEvent);
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(withdrawalDao, times(1))
                .save(captor.capture());
        assertThat(captor.getValue().getProviderId())
                .isEqualTo(providerId);
    }
}