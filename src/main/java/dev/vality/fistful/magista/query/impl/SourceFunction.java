package dev.vality.fistful.magista.query.impl;

import dev.vality.fistful.fistful_stat.StatResponse;
import dev.vality.fistful.fistful_stat.StatResponseData;
import dev.vality.fistful.fistful_stat.StatSource;
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


public class SourceFunction extends PagedBaseFunction<Map.Entry<Long, StatSource>, StatResponse>
        implements CompositeQuery<Map.Entry<Long, StatSource>, StatResponse> {

    public static final String FUNC_NAME = "sources";

    private final CompositeQuery<QueryResult, List<QueryResult>> subquery;

    private SourceFunction(
            Object descriptor,
            QueryParameters params,
            String continuationToken,
            CompositeQuery<QueryResult, List<QueryResult>> subquery) {
        super(descriptor, params, FUNC_NAME, continuationToken);
        this.subquery = subquery;
    }

    @Override
    public QueryResult<Map.Entry<Long, StatSource>, StatResponse> execute(QueryContext context)
            throws QueryExecutionException {
        QueryResult<QueryResult, List<QueryResult>> collectedResults = subquery.execute(context);

        return execute(context, collectedResults.getCollectedStream());
    }

    @Override
    public QueryResult<Map.Entry<Long, StatSource>, StatResponse> execute(
            QueryContext context,
            List<QueryResult> collectedResults) throws QueryExecutionException {
        QueryResult<Map.Entry<Long, StatSource>, List<Map.Entry<Long, StatSource>>> sourcesResult =
                (QueryResult<Map.Entry<Long, StatSource>, List<Map.Entry<Long, StatSource>>>) collectedResults
                        .get(0);

        return new BaseQueryResult<>(
                sourcesResult::getDataStream,
                () -> {
                    StatResponseData statResponseData = StatResponseData.sources(
                            sourcesResult.getDataStream()
                                    .map(Map.Entry::getValue)
                                    .collect(Collectors.toList())
                    );
                    StatResponse statResponse = new StatResponse(statResponseData);
                    List<Map.Entry<Long, StatSource>> sourceStats = sourcesResult.getCollectedStream();
                    if (!sourcesResult.getCollectedStream().isEmpty()
                            && getQueryParameters().getSize() == sourceStats.size()) {
                        statResponse.setContinuationToken(
                                TokenUtil.buildToken(
                                        getQueryParameters(),
                                        sourceStats.get(sourceStats.size() - 1).getKey()
                                )
                        );
                    }
                    return statResponse;
                }
        );
    }

    @Override
    public SourceParameters getQueryParameters() {
        return (SourceParameters) super.getQueryParameters();
    }

    @Override
    protected QueryParameters createQueryParameters(QueryParameters parameters, QueryParameters derivedParameters) {
        return new SourceParameters(parameters, derivedParameters);
    }

    @Override
    public List<Query> getChildQueries() {
        return subquery.getChildQueries();
    }

    @Override
    public boolean isParallel() {
        return subquery.isParallel();
    }

    public static class SourceParameters extends PagedBaseParameters {

        public SourceParameters(Map<String, Object> parameters, QueryParameters derivedParameters) {
            super(parameters, derivedParameters);
        }

        public SourceParameters(QueryParameters parameters, QueryParameters derivedParameters) {
            super(parameters, derivedParameters);
        }

        public String getSourceId() {
            return getStringParameter(Parameters.SOURCE_ID_PARAM, false);
        }

        public UUID getPartyId() {
            return UUID.fromString(getStringParameter(Parameters.PARTY_ID_PARAM, false));
        }

        public String getExternalId() {
            return getStringParameter(Parameters.EXTERNAL_ID_PARAM, false);
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

    public static class SourceValidator extends PagedBaseValidator {

        @Override
        public void validateParameters(QueryParameters parameters) throws IllegalArgumentException {
            super.validateParameters(parameters);
            SourceParameters sourceParameters = super.checkParamsType(parameters, SourceParameters.class);
            validateTimePeriod(sourceParameters.getFromTime(), sourceParameters.getToTime());
        }
    }

    public static class SourceParser extends AbstractQueryParser {
        private final SourceValidator validator = new SourceValidator();

        @Override
        public List<QueryPart> parseQuery(Map<String, Object> source, QueryPart parent) throws QueryParserException {
            Map<String, Object> funcSource = (Map) source.get(FUNC_NAME);
            SourceParameters parameters = getValidatedParameters(
                    funcSource,
                    parent,
                    SourceParameters::new, validator);

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

    public static class SourceBuilder extends AbstractQueryBuilder {
        private final SourceValidator validator = new SourceValidator();

        @Override
        public Query buildQuery(
                List<QueryPart> queryParts,
                String continuationToken,
                QueryPart parentQueryPart,
                QueryBuilder baseBuilder) throws QueryBuilderException {
            Query resultQuery = buildSingleQuery(SourceParser.getMainDescriptor(), queryParts,
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
            return createSourceFunction(queryPart.getDescriptor(), queryPart.getParameters(), continuationToken,
                    compositeQuery);
        }

        @Override
        public boolean apply(List<QueryPart> queryParts, QueryPart parent) {
            return getMatchedPartsStream(SourceParser.getMainDescriptor(), queryParts).findFirst().isPresent();
        }
    }

    private static SourceFunction createSourceFunction(
            Object descriptor,
            QueryParameters queryParameters,
            String continuationToken,
            CompositeQuery<QueryResult, List<QueryResult>> subquery) {
        SourceFunction sourceFunction = new SourceFunction(
                descriptor,
                queryParameters,
                continuationToken,
                subquery);
        subquery.setParentQuery(sourceFunction);
        return sourceFunction;
    }

    private static class GetDataFunction
            extends PagedBaseFunction<Map.Entry<Long, StatSource>, Collection<Map.Entry<Long, StatSource>>> {
        private static final String FUNC_NAME = SourceFunction.FUNC_NAME + "_data";

        public GetDataFunction(Object descriptor, QueryParameters params, String continuationToken) {
            super(descriptor, params, FUNC_NAME, continuationToken);
        }

        protected FunctionQueryContext getContext(QueryContext context) {
            return super.getContext(context, FunctionQueryContext.class);
        }

        @Override
        public QueryResult<Map.Entry<Long, StatSource>, Collection<Map.Entry<Long, StatSource>>> execute(
                QueryContext context) throws QueryExecutionException {
            FunctionQueryContext functionContext = getContext(context);
            SourceParameters parameters = new SourceParameters(
                    getQueryParameters(),
                    getQueryParameters().getDerivedParameters());

            try {
                Collection<Map.Entry<Long, StatSource>> result = functionContext.getSearchDao().getSources(
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
