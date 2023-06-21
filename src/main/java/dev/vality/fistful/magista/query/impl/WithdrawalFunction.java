package dev.vality.fistful.magista.query.impl;

import dev.vality.fistful.fistful_stat.StatResponse;
import dev.vality.fistful.fistful_stat.StatResponseData;
import dev.vality.fistful.fistful_stat.StatWithdrawal;
import dev.vality.fistful.magista.domain.enums.WithdrawalStatus;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.magista.dsl.*;
import dev.vality.magista.dsl.builder.AbstractQueryBuilder;
import dev.vality.magista.dsl.builder.QueryBuilder;
import dev.vality.magista.dsl.builder.QueryBuilderException;
import dev.vality.magista.dsl.parser.AbstractQueryParser;
import dev.vality.magista.dsl.parser.QueryParserException;
import dev.vality.magista.dsl.parser.QueryPart;

import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class WithdrawalFunction extends PagedBaseFunction<Map.Entry<Long, StatWithdrawal>, StatResponse>
        implements CompositeQuery<Map.Entry<Long, StatWithdrawal>, StatResponse> {

    public static final String FUNC_NAME = "withdrawals";

    private final CompositeQuery<QueryResult, List<QueryResult>> subquery;

    private WithdrawalFunction(
            Object descriptor,
            QueryParameters params,
            String continuationToken,
            CompositeQuery<QueryResult, List<QueryResult>> subquery) {
        super(descriptor, params, FUNC_NAME, continuationToken);
        this.subquery = subquery;
    }

    @Override
    public QueryResult<Map.Entry<Long, StatWithdrawal>, StatResponse> execute(QueryContext context)
            throws QueryExecutionException {
        QueryResult<QueryResult, List<QueryResult>> collectedResults = subquery.execute(context);

        return execute(context, collectedResults.getCollectedStream());
    }

    @Override
    public QueryResult<Map.Entry<Long, StatWithdrawal>, StatResponse> execute(
            QueryContext context,
            List<QueryResult> collectedResults) throws QueryExecutionException {
        QueryResult<Map.Entry<Long, StatWithdrawal>, List<Map.Entry<Long, StatWithdrawal>>> withdrawalsResult =
                (QueryResult<Map.Entry<Long, StatWithdrawal>, List<Map.Entry<Long, StatWithdrawal>>>) collectedResults
                        .get(0);

        return new BaseQueryResult<>(
                withdrawalsResult::getDataStream,
                () -> {
                    StatResponseData statResponseData = StatResponseData.withdrawals(
                            withdrawalsResult.getDataStream()
                                    .map(Map.Entry::getValue)
                                    .collect(Collectors.toList())
                    );
                    StatResponse statResponse = new StatResponse(statResponseData);
                    List<Map.Entry<Long, StatWithdrawal>> withdrawalStats = withdrawalsResult.getCollectedStream();
                    if (!withdrawalsResult.getCollectedStream().isEmpty()
                            && getQueryParameters().getSize() == withdrawalStats.size()) {
                        statResponse.setContinuationToken(
                                TokenUtil.buildToken(
                                        getQueryParameters(),
                                        withdrawalStats.get(withdrawalStats.size() - 1).getKey()
                                )
                        );
                    }
                    return statResponse;
                }
        );
    }

    @Override
    public WithdrawalParameters getQueryParameters() {
        return (WithdrawalParameters) super.getQueryParameters();
    }

    @Override
    protected QueryParameters createQueryParameters(QueryParameters parameters, QueryParameters derivedParameters) {
        return new WithdrawalParameters(parameters, derivedParameters);
    }

    @Override
    public List<Query> getChildQueries() {
        return subquery.getChildQueries();
    }

    @Override
    public boolean isParallel() {
        return subquery.isParallel();
    }

    private static final Map<String, WithdrawalStatus> statusesMap = Collections.unmodifiableMap(Stream.of(
            new AbstractMap.SimpleEntry<>("Pending", WithdrawalStatus.pending),
            new AbstractMap.SimpleEntry<>("Succeeded", WithdrawalStatus.succeeded),
            new AbstractMap.SimpleEntry<>("Failed", WithdrawalStatus.failed))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

    public static class WithdrawalParameters extends PagedBaseParameters {

        public WithdrawalParameters(Map<String, Object> parameters, QueryParameters derivedParameters) {
            super(parameters, derivedParameters);
        }

        public WithdrawalParameters(QueryParameters parameters, QueryParameters derivedParameters) {
            super(parameters, derivedParameters);
        }

        public String getPartyId() {
            return getStringParameter(Parameters.PARTY_ID_PARAM, false);
        }

        public String getWalletId() {
            return getStringParameter(Parameters.WALLET_ID_PARAM, false);
        }

        public List<String> getWithdrawalIds() {
            return getArrayParameter(Parameters.WITHDRAWAL_IDS_PARAM, false);
        }

        public String getWithdrawalId() {
            return getStringParameter(Parameters.WITHDRAWAL_ID_PARAM, false);
        }

        public String getIdentityId() {
            return getStringParameter(Parameters.IDENTITY_ID_PARAM, false);
        }

        public String getDestinationId() {
            return getStringParameter(Parameters.DESTINATION_ID_PARAM, false);
        }

        public WithdrawalStatus getStatus() {
            return statusesMap.get(getStringParameter(Parameters.STATUS_PARAM, false));
        }

        public String getExternalId() {
            return getStringParameter(Parameters.EXTERNAL_ID_PARAM, false);
        }


        public Long getAmountFrom() {
            return getLongParameter(Parameters.AMOUNT_FROM_PARAM, false);
        }

        public Long getAmountTo() {
            return getLongParameter(Parameters.AMOUNT_TO_PARAM, false);
        }

        public String getCurrencyCode() {
            return getStringParameter(Parameters.CURRENCY_CODE_PARAM, false);
        }

        public TemporalAccessor getFromTime() {
            return getTimeParameter(Parameters.FROM_TIME_PARAM, false);
        }

        public TemporalAccessor getToTime() {
            return getTimeParameter(Parameters.TO_TIME_PARAM, false);
        }
    }

    public static class WithdrawalValidator extends PagedBaseValidator {

        @Override
        public void validateParameters(QueryParameters parameters) throws IllegalArgumentException {
            super.validateParameters(parameters);
            WithdrawalParameters withdrawalParameters = super.checkParamsType(parameters, WithdrawalParameters.class);
            validateTimePeriod(withdrawalParameters.getFromTime(), withdrawalParameters.getToTime());
            String stringStatus = parameters.getStringParameter(Parameters.STATUS_PARAM, false);
            if (stringStatus != null && withdrawalParameters.getStatus() == null) {
                throw new IllegalArgumentException("Unknown withdrawal status: " + stringStatus);
            }
        }
    }

    public static class WithdrawalParser extends AbstractQueryParser {
        private final WithdrawalValidator validator = new WithdrawalValidator();

        @Override
        public List<QueryPart> parseQuery(Map<String, Object> source, QueryPart parent) throws QueryParserException {
            Map<String, Object> funcSource = (Map) source.get(FUNC_NAME);
            WithdrawalParameters parameters = getValidatedParameters(
                    funcSource,
                    parent,
                    WithdrawalParameters::new, validator);

            return Stream.of(
                    new QueryPart(FUNC_NAME, parameters, parent))
                    .collect(Collectors.toList());
        }

        @Override
        public boolean apply(Map source, QueryPart parent) {
            return parent != null
                    && RootQuery.RootParser.getMainDescriptor().equals(parent.getDescriptor())
                    && (source.get(FUNC_NAME) instanceof Map);
        }

        public static String getMainDescriptor() {
            return FUNC_NAME;
        }
    }

    public static class WithdrawalBuilder extends AbstractQueryBuilder {
        private final WithdrawalValidator validator = new WithdrawalValidator();

        @Override
        public Query buildQuery(
                List<QueryPart> queryParts,
                String continuationToken,
                QueryPart parentQueryPart,
                QueryBuilder baseBuilder) throws QueryBuilderException {
            Query resultQuery = buildSingleQuery(WithdrawalParser.getMainDescriptor(), queryParts,
                    queryPart -> createQuery(queryPart, continuationToken));
            validator.validateQuery(resultQuery);
            return resultQuery;
        }

        private CompositeQuery createQuery(QueryPart queryPart, String continuationToken) {
            List<Query> queries = Arrays.asList(
                    new GetDataFunction(queryPart.getDescriptor() + ":" + GetDataFunction.FUNC_NAME,
                            queryPart.getParameters(), continuationToken)
            );
            CompositeQuery<QueryResult, List<QueryResult>> compositeQuery = createCompositeQuery(
                    queryPart.getDescriptor(),
                    getParameters(queryPart.getParent()),
                    queries
            );
            return createWithdrawalFunction(queryPart.getDescriptor(), queryPart.getParameters(), continuationToken,
                    compositeQuery);
        }

        @Override
        public boolean apply(List<QueryPart> queryParts, QueryPart parent) {
            return getMatchedPartsStream(WithdrawalParser.getMainDescriptor(), queryParts).findFirst().isPresent();
        }
    }

    private static WithdrawalFunction createWithdrawalFunction(
            Object descriptor,
            QueryParameters queryParameters,
            String continuationToken,
            CompositeQuery<QueryResult, List<QueryResult>> subquery) {
        WithdrawalFunction withdrawalFunction = new WithdrawalFunction(
                descriptor,
                queryParameters,
                continuationToken,
                subquery);
        subquery.setParentQuery(withdrawalFunction);
        return withdrawalFunction;
    }

    private static class GetDataFunction
            extends PagedBaseFunction<Map.Entry<Long, StatWithdrawal>, Collection<Map.Entry<Long, StatWithdrawal>>> {
        private static final String FUNC_NAME = WithdrawalFunction.FUNC_NAME + "_data";

        public GetDataFunction(Object descriptor, QueryParameters params, String continuationToken) {
            super(descriptor, params, FUNC_NAME, continuationToken);
        }

        protected FunctionQueryContext getContext(QueryContext context) {
            return super.getContext(context, FunctionQueryContext.class);
        }

        @Override
        public QueryResult<Map.Entry<Long, StatWithdrawal>, Collection<Map.Entry<Long, StatWithdrawal>>> execute(
                QueryContext context) throws QueryExecutionException {
            FunctionQueryContext functionContext = getContext(context);
            WithdrawalParameters parameters = new WithdrawalParameters(
                    getQueryParameters(),
                    getQueryParameters().getDerivedParameters());

            try {
                Collection<Map.Entry<Long, StatWithdrawal>> result = functionContext.getSearchDao().getWithdrawals(
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

}
