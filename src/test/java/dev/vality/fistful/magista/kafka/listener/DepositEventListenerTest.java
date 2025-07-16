package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.CurrencyRef;
import dev.vality.fistful.deposit.*;
import dev.vality.fistful.deposit.adjustment.Adjustment;
import dev.vality.fistful.deposit.adjustment.ChangesPlan;
import dev.vality.fistful.deposit.revert.CreatedChange;
import dev.vality.fistful.deposit.revert.Revert;
import dev.vality.fistful.deposit.revert.status.Pending;
import dev.vality.fistful.deposit.status.Status;
import dev.vality.fistful.deposit.status.Succeeded;
import dev.vality.fistful.magista.config.KafkaPostgresqlSpringBootITest;
import dev.vality.fistful.magista.dao.DepositAdjustmentDao;
import dev.vality.fistful.magista.dao.DepositDao;
import dev.vality.fistful.magista.dao.DepositRevertDao;
import dev.vality.fistful.magista.domain.enums.DepositAdjustmentDataStatus;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataStatus;
import dev.vality.fistful.magista.domain.enums.DepositStatus;
import dev.vality.fistful.magista.domain.tables.pojos.DepositAdjustmentData;
import dev.vality.fistful.magista.domain.tables.pojos.DepositData;
import dev.vality.fistful.magista.domain.tables.pojos.DepositRevertData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import static dev.vality.fistful.magista.data.TestData.machineEvent;
import static dev.vality.fistful.magista.data.TestData.sinkEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@KafkaPostgresqlSpringBootITest
public class DepositEventListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;

    @MockBean
    private DepositDao depositDao;

    @MockBean
    private DepositRevertDao depositRevertDao;

    @MockBean
    private DepositAdjustmentDao depositAdjustmentDao;

    @Captor
    private ArgumentCaptor<DepositData> depositDataArgumentCaptor;

    @Captor
    private ArgumentCaptor<DepositRevertData> depositRevertDataArgumentCaptor;

    @Captor
    private ArgumentCaptor<DepositAdjustmentData> depositAdjustmentDataArgumentCaptor;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Test
    public void shouldDepositListenAndSave() throws InterruptedException, DaoException {
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
        testThriftKafkaProducer.send("mg-events-ff-deposit", sinkEvent);
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(depositDao, times(1))
                .save(depositDataArgumentCaptor.capture());
        assertEquals(DepositStatus.succeeded, depositDataArgumentCaptor.getValue().getDepositStatus());
    }

    @Test
    public void shouldDepositRevertCreatedListenAndSave() throws InterruptedException, DaoException {
        // Given
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.revert(
                        new RevertChange("revert_id", dev.vality.fistful.deposit.revert.Change.created(
                                new CreatedChange(
                                        new Revert("revert_id", "wallet_id", "source_id",
                                                getRevertPending(),
                                                new Cash(123L, new CurrencyRef("RUB")),
                                                "2016-03-22T06:12:27Z", 1L, 1L))))));

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        change));

        when(depositDao.get("source_id"))
                .thenReturn(new DepositData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-deposit", sinkEvent);
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(depositRevertDao, times(1))
                .save(depositRevertDataArgumentCaptor.capture());
        assertEquals(DepositRevertDataStatus.pending, depositRevertDataArgumentCaptor.getValue().getStatus());
    }

    private dev.vality.fistful.deposit.revert.status.Status getRevertPending() {
        return dev.vality.fistful.deposit.revert.status.Status.pending(new Pending());
    }

    @Test
    public void shouldDepositRevertStatusListenAndSave() throws InterruptedException, DaoException {
        // Given
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.revert(
                        new RevertChange("revert_id", dev.vality.fistful.deposit.revert.Change.status_changed(
                                new dev.vality.fistful.deposit.revert.StatusChange(getRevertSucceeded())))));

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        change));

        when(depositRevertDao.get("source_id", "revert_id"))
                .thenReturn(new DepositRevertData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-deposit", sinkEvent);
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(depositRevertDao, times(1))
                .save(depositRevertDataArgumentCaptor.capture());
        assertEquals(DepositRevertDataStatus.succeeded, depositRevertDataArgumentCaptor.getValue().getStatus());
    }

    private dev.vality.fistful.deposit.revert.status.Status getRevertSucceeded() {
        return dev.vality.fistful.deposit.revert.status.Status.succeeded(
                new dev.vality.fistful.deposit.revert.status.Succeeded());
    }

    @Test
    public void shouldDepositAdjustmentCreatedListenAndSave() throws InterruptedException, DaoException {
        // Given
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.adjustment(
                        new AdjustmentChange("adjustment_id", dev.vality.fistful.deposit.adjustment.Change.created(
                                new dev.vality.fistful.deposit.adjustment.CreatedChange(
                                        new Adjustment("adjustment_id", dev.vality.fistful.deposit.adjustment.Status
                                                .pending(new dev.vality.fistful.deposit.adjustment.Pending()),
                                                new ChangesPlan(), "2016-03-22T06:12:27Z", 1L, 1L,
                                                "2016-03-22T06:12:27Z"))))));

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        change));

        when(depositDao.get("source_id"))
                .thenReturn(new DepositData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-deposit", sinkEvent);
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(depositAdjustmentDao, times(1))
                .save(depositAdjustmentDataArgumentCaptor.capture());
        assertEquals(DepositAdjustmentDataStatus.pending, depositAdjustmentDataArgumentCaptor.getValue().getStatus());
    }

    @Test
    public void shouldDepositAdjustmentStatusListenAndSave() throws InterruptedException, DaoException {
        // Given
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(getAdjustment());

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        change));

        when(depositAdjustmentDao.get("source_id", "adjustment_id"))
                .thenReturn(new DepositAdjustmentData());

        // When
        testThriftKafkaProducer.send("mg-events-ff-deposit", sinkEvent);
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        verify(depositAdjustmentDao, times(1))
                .save(depositAdjustmentDataArgumentCaptor.capture());
        assertEquals(DepositAdjustmentDataStatus.succeeded, depositAdjustmentDataArgumentCaptor.getValue().getStatus());
    }

    private Change getAdjustment() {
        return Change.adjustment(
                new AdjustmentChange(
                        "adjustment_id",
                        dev.vality.fistful.deposit.adjustment.Change.status_changed(
                                new dev.vality.fistful.deposit.adjustment.StatusChange(getAdjustmentSucceeded()))));
    }

    private dev.vality.fistful.deposit.adjustment.Status getAdjustmentSucceeded() {
        return dev.vality.fistful.deposit.adjustment.Status
                .succeeded(new dev.vality.fistful.deposit.adjustment.Succeeded());
    }
}
