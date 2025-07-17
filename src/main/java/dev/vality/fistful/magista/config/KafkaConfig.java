package dev.vality.fistful.magista.config;

import dev.vality.fistful.magista.kafka.serde.SinkEventDeserializer;
import dev.vality.kafka.common.util.ExponentialBackOffDefaultErrorHandlerFactory;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

import java.util.Map;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> depositEventListenerContainerFactory() {
        return listenerContainerFactory();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> identityEventListenerContainerFactory() {
        return listenerContainerFactory();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> walletEventListenerContainerFactory() {
        return listenerContainerFactory();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> withdrawalEventListenerContainerFactory() {
        return listenerContainerFactory();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> sourceEventListenerContainerFactory() {
        return listenerContainerFactory();
    }

    private ConcurrentKafkaListenerContainerFactory<String, MachineEvent> listenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MachineEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        DefaultKafkaConsumerFactory<String, MachineEvent> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerConfig());

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(kafkaProperties.getListener().getConcurrency());
        factory.setCommonErrorHandler(kafkaErrorHandler());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    private DefaultErrorHandler kafkaErrorHandler() {
        return ExponentialBackOffDefaultErrorHandlerFactory.create();
    }

    private Map<String, Object> consumerConfig() {
        Map<String, Object> config = kafkaProperties.buildConsumerProperties();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SinkEventDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST.name().toLowerCase());
        return config;
    }

}
