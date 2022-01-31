package dev.vality.fistful.magista.query.impl;


import dev.vality.fistful.magista.dao.SearchDao;
import dev.vality.magista.dsl.QueryContext;

public class FunctionQueryContext implements QueryContext {
    private final SearchDao searchDao;

    public FunctionQueryContext(SearchDao searchDao) {
        this.searchDao = searchDao;
    }

    public SearchDao getSearchDao() {
        return searchDao;
    }
}
