package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.magista.config.KafkaPostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.WalletDao;
import dev.vality.fistful.magista.domain.tables.pojos.WalletData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.wallet.Change;
import dev.vality.fistful.wallet.TimestampedChange;
import dev.vality.fistful.wallet.Wallet;
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
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@KafkaPostgresqlSpringBootITest
public class WalletEventListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @MockitoBean
    private WalletDao walletDao;

    @Captor
    private ArgumentCaptor<WalletData> captor;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

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
        testThriftKafkaProducer.send("mg-events-ff-wallet", sinkEvent);

        // Then
        verify(walletDao, timeout(MESSAGE_TIMEOUT).times(1))
                .save(captor.capture());
        assertThat(captor.getValue().getWalletName())
                .isEqualTo("wallet");
    }
}