package dev.vality.fistful.magista.query.impl.builder;

import dev.vality.fistful.magista.query.impl.DepositAdjustmentFunction;
import dev.vality.fistful.magista.query.impl.data.DepositAdjustmentDataFunction;
import dev.vality.fistful.magista.query.impl.validator.DepositAdjustmentValidator;
import dev.vality.magista.dsl.CompositeQuery;
import dev.vality.magista.dsl.Query;
import dev.vality.magista.dsl.QueryResult;
import dev.vality.magista.dsl.builder.AbstractQueryBuilder;
import dev.vality.magista.dsl.builder.QueryBuilder;
import dev.vality.magista.dsl.builder.QueryBuilderException;
import dev.vality.magista.dsl.parser.QueryPart;

import java.util.Arrays;
import java.util.List;

public class DepositAdjustmentBuilder extends AbstractQueryBuilder {

    private final DepositAdjustmentValidator validator = new DepositAdjustmentValidator();

    @Override
    public Query buildQuery(
            List<QueryPart> queryParts,
            String continuationToken,
            QueryPart parentQueryPart,
            QueryBuilder baseBuilder) throws QueryBuilderException {
        Query resultQuery = buildSingleQuery(
                DepositAdjustmentFunction.getMainDescriptor(),
                queryParts,
                queryPart -> createQuery(queryPart, continuationToken)
        );
        validator.validateQuery(resultQuery);
        return resultQuery;
    }

    private CompositeQuery createQuery(QueryPart queryPart, String continuationToken) {
        List<Query> queries = Arrays.asList(
                new DepositAdjustmentDataFunction(
                        queryPart.getDescriptor() + ":" + DepositAdjustmentFunction.getMainDescriptor(),
                        queryPart.getParameters(),
                        continuationToken
                )
        );
        CompositeQuery<QueryResult, List<QueryResult>> compositeQuery = createCompositeQuery(
                queryPart.getDescriptor(),
                getParameters(queryPart.getParent()),
                queries
        );
        return DepositAdjustmentFunction
                .createFunction(queryPart.getDescriptor(), queryPart.getParameters(), continuationToken,
                        compositeQuery);
    }

    @Override
    public boolean apply(List<QueryPart> queryParts, QueryPart parent) {
        return getMatchedPartsStream(DepositAdjustmentFunction.getMainDescriptor(), queryParts)
                .findFirst()
                .isPresent();
    }
}
