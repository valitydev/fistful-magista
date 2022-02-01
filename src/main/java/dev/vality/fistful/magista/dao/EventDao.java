package dev.vality.fistful.magista.dao;

import dev.vality.fistful.magista.exception.DaoException;

import java.util.Optional;

public interface EventDao extends GenericDao {

    Optional<Long> getLastEventId() throws DaoException;

}
