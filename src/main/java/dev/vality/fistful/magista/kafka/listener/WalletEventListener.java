package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.magista.service.WalletEventService;
import dev.vality.machinegun.eventsink.SinkEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletEventListener {

    @Value("${kafka.retry-delay-ms}")
    private int retryDelayMs;

    private final WalletEventService walletEventService;

    @KafkaListener(
            autoStartup = "${kafka.topic.wallet.listener.enabled}",
            topics = "${kafka.topic.wallet.name}",
            containerFactory = "walletEventListenerContainerFactory")
    public void listen(
            List<SinkEvent> batch,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset,
            Acknowledgment ack) throws InterruptedException {
        log.info("Listening Wallet: partition={}, offset={}, batch.size()={}", partition, offset, batch.size());
        walletEventService.handleEvents(batch.stream().map(SinkEvent::getEvent).collect(toList()));
        ack.acknowledge();
        log.info("Ack Wallet: partition={}, offset={}", partition, offset);
    }
}
