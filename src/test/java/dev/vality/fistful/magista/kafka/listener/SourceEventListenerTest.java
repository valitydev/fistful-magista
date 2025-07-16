package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.magista.FistfulMagistaApplication;
import dev.vality.fistful.magista.dao.SourceDao;
import dev.vality.fistful.magista.domain.enums.SourceStatus;
import dev.vality.fistful.magista.domain.tables.pojos.SourceData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.source.*;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@Slf4j
@DirtiesContext
@SpringBootTest(
        classes = FistfulMagistaApplication.class,
        properties = {"kafka.state.cache.size=0"})
public class SourceEventListenerTest extends AbstractListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @MockBean
    private SourceDao sourceDao;

    @Captor
    private ArgumentCaptor<SourceData> captor;

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
        produce(sinkEvent, "mg-events-ff-source");
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(sourceDao, times(1))
                .save(captor.capture());
        assertThat(captor.getValue().getStatus())
                .isEqualTo(SourceStatus.authorized);
    }
}