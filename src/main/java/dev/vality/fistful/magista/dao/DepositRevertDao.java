package dev.vality.fistful.magista.dao;

import dev.vality.fistful.magista.domain.tables.pojos.DepositRevertData;
import dev.vality.fistful.magista.exception.DaoException;

import java.util.Optional;

public interface DepositRevertDao extends GenericDao {

    Optional<Long> save(DepositRevertData depositRevertData) throws DaoException;

    DepositRevertData get(String depositId, String revertId) throws DaoException;

}
