package dev.vality.fistful.magista.handler.withdrawal;

import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.CurrencyRef;
import dev.vality.fistful.magista.dao.WithdrawalDao;
import dev.vality.fistful.magista.domain.tables.pojos.WithdrawalData;
import dev.vality.fistful.withdrawal.BodyChange;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WithdrawalBodyChangedEventHandlerTest {

    @Test
    void shouldSaveNewBodyAmount() throws Exception {
        WithdrawalDao withdrawalDao = mock(WithdrawalDao.class);
        WithdrawalData withdrawalData = new WithdrawalData();
        withdrawalData.setAmount(1000L);
        withdrawalData.setCurrencyCode("RUB");
        when(withdrawalDao.get("withdrawal_id")).thenReturn(withdrawalData);

        WithdrawalBodyChangedEventHandler handler = new WithdrawalBodyChangedEventHandler(withdrawalDao);
        TimestampedChange change = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.body_changed(
                        new BodyChange()
                                .setOldBody(new Cash()
                                        .setAmount(1000L)
                                        .setCurrency(new CurrencyRef("RUB")))
                                .setNewBody(new Cash()
                                        .setAmount(2000L)
                                        .setCurrency(new CurrencyRef("USD")))));
        MachineEvent event = new MachineEvent()
                .setEventId(2L)
                .setSourceId("withdrawal_id")
                .setCreatedAt("2016-03-22T06:13:27Z");

        handler.handle(change, event);

        ArgumentCaptor<WithdrawalData> captor = ArgumentCaptor.forClass(WithdrawalData.class);
        verify(withdrawalDao).save(captor.capture());
        assertThat(captor.getValue().getAmount()).isEqualTo(1000L);
        assertThat(captor.getValue().getCurrencyCode()).isEqualTo("RUB");
        assertThat(captor.getValue().getChangedAmount()).isEqualTo(2000L);
        assertThat(captor.getValue().getChangedCurrencyCode()).isEqualTo("USD");
    }
}
