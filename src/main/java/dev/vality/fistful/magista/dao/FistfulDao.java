package dev.vality.fistful.magista.dao;

import dev.vality.fistful.magista.exception.DaoException;

public interface FistfulDao<T> extends EventDao {

    long save(T object) throws DaoException;

    T get(String id) throws DaoException;

}
