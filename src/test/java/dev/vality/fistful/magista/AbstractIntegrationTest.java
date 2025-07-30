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

public abstract class AbstractIntegrationTest {

    protected QueryProcessorImpl queryProcessor;

    @Autowired
    private SearchDao searchDao;

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
}
