package dev.vality.fistful.magista.query.impl.data;

import dev.vality.fistful.fistful_stat.StatDepositRevert;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.query.impl.DepositRevertFunction;
import dev.vality.fistful.magista.query.impl.FunctionQueryContext;
import dev.vality.fistful.magista.query.impl.parameters.DepositRevertParameters;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.magista.dsl.*;

import java.util.Collection;
import java.util.Map;

public class DepositRevertDataFunction
        extends PagedBaseFunction<Map.Entry<Long, StatDepositRevert>, Collection<Map.Entry<Long, StatDepositRevert>>> {

    private static final String FUNC_NAME = DepositRevertFunction.getMainDescriptor() + "_data";

    public DepositRevertDataFunction(Object descriptor, QueryParameters params, String continuationToken) {
        super(descriptor, params, FUNC_NAME, continuationToken);
    }

    protected FunctionQueryContext getContext(QueryContext context) {
        return super.getContext(context, FunctionQueryContext.class);
    }

    @Override
    public QueryResult<Map.Entry<Long, StatDepositRevert>, Collection<Map.Entry<Long, StatDepositRevert>>> execute(
            QueryContext context) throws QueryExecutionException {
        FunctionQueryContext functionContext = getContext(context);
        DepositRevertParameters parameters = new DepositRevertParameters(
                getQueryParameters(),
                getQueryParameters().getDerivedParameters());

        try {
            Collection<Map.Entry<Long, StatDepositRevert>> result = functionContext.getSearchDao().getDepositReverts(
                    parameters,
                    TypeUtil.toLocalDateTime(parameters.getFromTime()),
                    TypeUtil.toLocalDateTime(parameters.getToTime()),
                    getFromId().orElse(null),
                    parameters.getSize()
            );
            return new BaseQueryResult<>(result::stream, () -> result);
        } catch (DaoException e) {
            throw new QueryExecutionException(e);
        }
    }
}
