package dev.vality.fistful.magista.query.impl.builder;

import dev.vality.fistful.magista.query.impl.SourceFunction;
import dev.vality.fistful.magista.query.impl.WithdrawalFunction;
import dev.vality.magista.dsl.RootQuery;
import dev.vality.magista.dsl.builder.BaseQueryBuilder;
import dev.vality.magista.dsl.builder.QueryBuilder;
import dev.vality.magista.dsl.parser.QueryPart;

import java.util.Arrays;
import java.util.List;

public class QueryBuilderImpl extends BaseQueryBuilder {

    public QueryBuilderImpl() {
        this(
                Arrays.asList(
                        new RootQuery.RootBuilder(),
                        new WithdrawalFunction.WithdrawalBuilder(),
                        new DepositBuilder(),
                        new SourceFunction.SourceBuilder()
                )
        );
    }

    public QueryBuilderImpl(List<QueryBuilder> parsers) {
        super(parsers);
    }

    @Override
    public boolean apply(List<QueryPart> queryParts, QueryPart parent) {
        return true;
    }
}
