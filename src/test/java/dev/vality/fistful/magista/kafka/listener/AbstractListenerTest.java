package dev.vality.fistful.magista.kafka.listener;

import dev.vality.fistful.Blocking;
import dev.vality.fistful.base.EventRange;
import dev.vality.fistful.identity.IdentityState;
import dev.vality.fistful.identity.ManagementSrv;
import dev.vality.fistful.magista.kafka.serde.SinkEventDeserializer;
import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.machinegun.msgpack.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.mockito.Mockito.*;

@Testcontainers
@Slf4j
@ContextConfiguration(initializers = {
        AbstractListenerTest.KafkaInitializer.class})
public abstract class AbstractListenerTest {

    @Container
    public static KafkaContainer kafka = new KafkaContainer("apache/kafka");

    @Container
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer postgres = new PostgreSQLContainer<>("postgres:17");

    @MockBean
    private ManagementSrv.Iface identityManagementClient;

    @BeforeEach
    public void setUp() throws Exception {
        IdentityState identityState = new IdentityState("name", UUID.randomUUID().toString(), "provider");
        identityState.setBlocking(Blocking.unblocked);
        identityState.setExternalId("id");
        when(identityManagementClient.get(anyString(), any(EventRange.class))).thenReturn(identityState);
    }

    public static class KafkaInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers())
                    .applyTo(configurableApplicationContext.getEnvironment());
            initTopic("mg-events-ff-deposit");
            initTopic("mg-events-ff-wallet");
            initTopic("mg-events-ff-withdrawal");
            initTopic("mg-events-ff-identity");
            initTopic("mg-events-ff-source");
            kafka.start();
        }

        private void initTopic(String topicName) {
            Consumer<String, MachineEvent> consumer = consumer();
            try {
                consumer.subscribe(Collections.singletonList(topicName));
                consumer.poll(Duration.ofMillis(500L));
            } catch (Exception e) {
                log.error("AbstractKafkaTest initialization error", e);
            }

            consumer.close();
        }
    }

    @DynamicPropertySource
    static void dataSourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("flyway.url", postgres::getJdbcUrl);
        registry.add("flyway.user", postgres::getUsername);
        registry.add("flyway.password", postgres::getPassword);

        registry.add("kafka.topic.deposit.listener.enabled", Boolean.TRUE::toString);
        registry.add("kafka.topic.withdrawal.listener.enabled", Boolean.TRUE::toString);
        registry.add("kafka.topic.wallet.listener.enabled", Boolean.TRUE::toString);
        registry.add("kafka.topic.source.listener.enabled", Boolean.TRUE::toString);
        registry.add("kafka.topic.identity.listener.enabled", Boolean.TRUE::toString);
    }

    public static <T> Producer<String, T> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fistful-magista");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ThriftSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public static <T> Consumer<String, T> consumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SinkEventDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }

    public void produce(SinkEvent event, String topic) {
        try (Producer<String, SinkEvent> producer = producer()) {
            ProducerRecord<String, SinkEvent> producerRecord = new ProducerRecord<>(
                    topic,
                    event.getEvent().getSourceId(),
                    event);
            producer.send(producerRecord).get();
            log.info("produce to {}: {}", topic, event);
        } catch (Exception e) {
            log.error("Error while producing to {}", topic, e);
        }
    }


    protected SinkEvent sinkEvent(MachineEvent machineEvent) {
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);
        return sinkEvent;
    }

    protected <T extends TBase> MachineEvent machineEvent(
            ThriftSerializer<T> depositChangeSerializer,
            T change) {
        return new MachineEvent()
                .setEventId(1L)
                .setSourceId("source_id")
                .setSourceNs("source_ns")
                .setCreatedAt("2016-03-22T06:12:27Z")
                .setData(Value.bin(depositChangeSerializer.serialize("", change)));
    }
}
