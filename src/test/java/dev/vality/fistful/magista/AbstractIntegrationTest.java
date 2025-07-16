package dev.vality.fistful.magista;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.fistful.magista.dao.SearchDao;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.query.impl.QueryContextFactoryImpl;
import dev.vality.fistful.magista.query.impl.QueryProcessorImpl;
import dev.vality.fistful.magista.query.impl.builder.QueryBuilderImpl;
import dev.vality.fistful.magista.query.impl.parser.QueryParserImpl;
import dev.vality.magista.dsl.parser.JsonQueryParser;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@Testcontainers
@SpringBootTest(webEnvironment = RANDOM_PORT)
@TestPropertySource(properties = {"fistful.polling.enabled=false"})
@ContextConfiguration(classes = FistfulMagistaApplication.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class AbstractIntegrationTest {

    protected QueryProcessorImpl queryProcessor;

    @Autowired
    private SearchDao searchDao;

    @Value("${local.server.port}")
    protected int port;

    @Container
    public static PostgreSQLContainer postgres = new PostgreSQLContainer<>("postgres:17");

    @BeforeEach
    public void before() throws DaoException {
        QueryContextFactoryImpl contextFactory = new QueryContextFactoryImpl(searchDao);
        queryProcessor = new QueryProcessorImpl(
                new JsonQueryParser() {
                    @Override
                    protected ObjectMapper getMapper() {
                        ObjectMapper mapper = super.getMapper();
                        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
                        return mapper;
                    }
                }
                        .withQueryParser(new QueryParserImpl()),
                new QueryBuilderImpl(),
                contextFactory
        );
    }

    @DynamicPropertySource
    static void dataSourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("flyway.url", postgres::getJdbcUrl);
        registry.add("flyway.user", postgres::getUsername);
        registry.add("flyway.password", postgres::getPassword);
    }
}
