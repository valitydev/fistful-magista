package dev.vality.fistful.magista.query.impl;


import dev.vality.fistful.magista.dao.SearchDao;
import dev.vality.magista.dsl.QueryContext;
import dev.vality.magista.dsl.QueryContextFactory;

public class QueryContextFactoryImpl implements QueryContextFactory {

    private final SearchDao searchDao;

    public QueryContextFactoryImpl(SearchDao searchDao) {
        this.searchDao = searchDao;
    }

    @Override
    public QueryContext getContext() {
        return new FunctionQueryContext(searchDao);
    }
}
