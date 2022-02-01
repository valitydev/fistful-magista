package dev.vality.fistful.magista.query.impl.builder;

import dev.vality.fistful.magista.query.impl.DepositFunction;
import dev.vality.fistful.magista.query.impl.data.DepositDataFunction;
import dev.vality.fistful.magista.query.impl.validator.DepositValidator;
import dev.vality.magista.dsl.CompositeQuery;
import dev.vality.magista.dsl.Query;
import dev.vality.magista.dsl.QueryResult;
import dev.vality.magista.dsl.builder.AbstractQueryBuilder;
import dev.vality.magista.dsl.builder.QueryBuilder;
import dev.vality.magista.dsl.builder.QueryBuilderException;
import dev.vality.magista.dsl.parser.QueryPart;

import java.util.Arrays;
import java.util.List;

public class DepositBuilder extends AbstractQueryBuilder {

    private DepositValidator validator = new DepositValidator();

    @Override
    public Query buildQuery(
            List<QueryPart> queryParts,
            String continuationToken,
            QueryPart parentQueryPart,
            QueryBuilder baseBuilder) throws QueryBuilderException {
        Query resultQuery = buildSingleQuery(
                DepositFunction.getMainDescriptor(),
                queryParts,
                queryPart -> createQuery(queryPart, continuationToken)
        );
        validator.validateQuery(resultQuery);
        return resultQuery;
    }

    private CompositeQuery createQuery(QueryPart queryPart, String continuationToken) {
        List<Query> queries = Arrays.asList(
                new DepositDataFunction(
                        queryPart.getDescriptor() + ":" + DepositFunction.getMainDescriptor(),
                        queryPart.getParameters(),
                        continuationToken
                )
        );
        CompositeQuery<QueryResult, List<QueryResult>> compositeQuery = createCompositeQuery(
                queryPart.getDescriptor(),
                getParameters(queryPart.getParent()),
                queries
        );
        return DepositFunction.createFunction(queryPart.getDescriptor(), queryPart.getParameters(), continuationToken,
                compositeQuery);
    }

    @Override
    public boolean apply(List<QueryPart> queryParts, QueryPart parent) {
        return getMatchedPartsStream(DepositFunction.getMainDescriptor(), queryParts)
                .findFirst()
                .isPresent();
    }
}
