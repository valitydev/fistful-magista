package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.magista.FistfulMagistaApplication;
import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.domain.enums.WithdrawalStatus;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.StatusChange;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.fistful.withdrawal.status.Succeeded;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
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
@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = FistfulMagistaApplication.class,
        properties = {"kafka.state.cache.size=0"})
public class WithdrawalEventListenerTest extends AbstractListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @MockBean
    private WithdrawalDao withdrawalDao;

    @Captor
    private ArgumentCaptor<WithdrawalData> captor;

    @Test
    public void shouldListenAndSave() throws InterruptedException, DaoException {
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

        when(withdrawalDao.get("source_id"))
                .thenReturn(new WithdrawalData());

        // When
        produce(sinkEvent, "mg-events-ff-withdrawal");
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(withdrawalDao, times(1))
                .save(captor.capture());
        assertThat(captor.getValue().getWithdrawalStatus())
                .isEqualTo(WithdrawalStatus.succeeded);
    }
}