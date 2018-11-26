package com.rbkmoney.fistful.magista.listener;

import com.rbkmoney.eventstock.client.DefaultSubscriberConfig;
import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventPublisher;
import com.rbkmoney.eventstock.client.SubscriberConfig;
import com.rbkmoney.eventstock.client.poll.EventFlowFilter;
import com.rbkmoney.fistful.magista.service.impl.IdentityEventService;
import com.rbkmoney.fistful.magista.service.impl.WalletEventService;
import com.rbkmoney.fistful.magista.service.impl.WithdrawalEventService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class OnStart implements ApplicationListener<ApplicationReadyEvent> {
    private final EventPublisher identityEventPublisher;
    private final EventPublisher withdrawalEventPublisher;
    private final EventPublisher walletEventPublisher;

    private final WalletEventService walletEventService;
    private final IdentityEventService identityEventService;
    private final WithdrawalEventService withdrawalEventService;

    @Value("${fistful.polling.enabled:true}")
    private boolean pollingEnabled;

    public OnStart(EventPublisher identityEventPublisher, EventPublisher withdrawalEventPublisher, EventPublisher walletEventPublisher, WalletEventService walletEventService, IdentityEventService identityEventService, WithdrawalEventService withdrawalEventService) {
        this.identityEventPublisher = identityEventPublisher;
        this.withdrawalEventPublisher = withdrawalEventPublisher;
        this.walletEventPublisher = walletEventPublisher;
        this.walletEventService = walletEventService;
        this.identityEventService = identityEventService;
        this.withdrawalEventService = withdrawalEventService;
    }


    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        if (pollingEnabled) {
            identityEventPublisher.subscribe(buildSubscriberConfig(identityEventService.getLastEventId()));
            walletEventPublisher.subscribe(buildSubscriberConfig(walletEventService.getLastEventId()));
            withdrawalEventPublisher.subscribe(buildSubscriberConfig(withdrawalEventService.getLastEventId()));
        }
    }

    private SubscriberConfig buildSubscriberConfig(Optional<Long> lastEventIdOptional) {
        EventConstraint.EventIDRange eventIDRange = new EventConstraint.EventIDRange();
        lastEventIdOptional.ifPresent(eventIDRange::setFromExclusive);
        EventFlowFilter eventFlowFilter = new EventFlowFilter(new EventConstraint(eventIDRange));
        return new DefaultSubscriberConfig(eventFlowFilter);
    }
}
