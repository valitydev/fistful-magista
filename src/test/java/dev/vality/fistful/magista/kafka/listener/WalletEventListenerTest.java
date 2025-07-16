package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.magista.FistfulMagistaApplication;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.wallet.Change;
import dev.vality.fistful.wallet.TimestampedChange;
import dev.vality.fistful.wallet.Wallet;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Slf4j
@DirtiesContext
@SpringBootTest(
        classes = FistfulMagistaApplication.class,
        properties = {"kafka.state.cache.size=0"})
public class WalletEventListenerTest extends AbstractListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @MockBean
    private WalletDao walletDao;

    @Captor
    private ArgumentCaptor<WalletData> captor;

    @Test
    public void shouldListenAndSave() throws InterruptedException, DaoException {
        // Given
        TimestampedChange created = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.created(new Wallet()
                        .setName("wallet")));

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        created));

        // When
        produce(sinkEvent, "mg-events-ff-wallet");
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(walletDao, times(1))
                .save(captor.capture());
        assertThat(captor.getValue().getWalletName())
                .isEqualTo("wallet");
    }
}