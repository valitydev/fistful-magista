package dev.vality.fistful.magista.config;

import dev.vality.testcontainers.annotations.KafkaTestConfig;
import dev.vality.testcontainers.annotations.kafka.KafkaTestcontainerSingleton;
import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest
@PostgresqlTestcontainerSingleton
@KafkaTestcontainerSingleton(
        properties = {
                "kafka.topic.deposit.listener.enabled=true",
                "kafka.topic.withdrawal.listener.enabled=true",
                "kafka.topic.source.listener.enabled=true"},
        topicsKeys = {
                "kafka.topic.deposit.name",
                "kafka.topic.withdrawal.name",
                "kafka.topic.source.name"})
@KafkaTestConfig
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public @interface KafkaPostgresqlSpringBootITest {
}
