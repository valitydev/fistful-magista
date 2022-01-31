package dev.vality.fistful.magista.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.fistful.fistful_stat.FistfulStatisticsSrv;
import dev.vality.fistful.magista.dao.SearchDao;
import dev.vality.fistful.magista.query.impl.QueryContextFactoryImpl;
import dev.vality.fistful.magista.query.impl.QueryProcessorImpl;
import dev.vality.fistful.magista.query.impl.builder.QueryBuilderImpl;
import dev.vality.fistful.magista.query.impl.parser.QueryParserImpl;
import dev.vality.fistful.magista.service.FistfulStatisticsHandler;
import dev.vality.magista.dsl.parser.JsonQueryParser;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HandlerConfig {

    @Bean
    public FistfulStatisticsSrv.Iface fistfulStatisticsHandler(SearchDao searchDao) {
        JsonQueryParser jsonQueryParser = getJsonQueryParser()
                .withQueryParser(new QueryParserImpl());
        return new FistfulStatisticsHandler(
                new QueryProcessorImpl(
                        jsonQueryParser,
                        new QueryBuilderImpl(),
                        new QueryContextFactoryImpl(searchDao)
                )
        );
    }

    private JsonQueryParser getJsonQueryParser() {
        return new JsonQueryParser() {

            @Override
            protected ObjectMapper getMapper() {
                ObjectMapper mapper = super.getMapper();
                mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
                return mapper;
            }
        };
    }
}
