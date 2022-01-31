package dev.vality.fistful.magista.query.impl.parser;

import dev.vality.fistful.magista.query.impl.WalletFunction;
import dev.vality.fistful.magista.query.impl.WithdrawalFunction;
import dev.vality.magista.dsl.RootQuery;
import dev.vality.magista.dsl.parser.BaseQueryParser;
import dev.vality.magista.dsl.parser.QueryParser;
import dev.vality.magista.dsl.parser.QueryPart;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class QueryParserImpl extends BaseQueryParser {

    public QueryParserImpl() {
        this(
                Arrays.asList(
                        new RootQuery.RootParser(),
                        new WalletFunction.WalletParser(),
                        new WithdrawalFunction.WithdrawalParser(),
                        new DepositParser(),
                        new IdentityParser(),
                        new DepositRevertParser(),
                        new DepositAdjustmentParser()
                )
        );
    }

    public QueryParserImpl(List<QueryParser<Map<String, Object>>> parsers) {
        super(parsers);
    }

    @Override
    public boolean apply(Map source, QueryPart parent) {
        return true;
    }
}
