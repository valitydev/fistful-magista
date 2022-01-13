package com.rbkmoney.fistful.magista.query.impl.builder;

import com.rbkmoney.fistful.magista.query.impl.WalletFunction;
import com.rbkmoney.fistful.magista.query.impl.WithdrawalFunction;
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
                        new WalletFunction.WalletBuilder(),
                        new WithdrawalFunction.WithdrawalBuilder(),
                        new DepositBuilder(),
                        new IdentityBuilder(),
                        new DepositRevertBuilder(),
                        new DepositAdjustmentBuilder()
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
