package dev.vality.fistful.magista.dao;

import dev.vality.fistful.magista.domain.tables.pojos.DepositAdjustmentData;
import dev.vality.fistful.magista.exception.DaoException;

import java.util.Optional;

public interface DepositAdjustmentDao extends GenericDao {

    Optional<Long> save(DepositAdjustmentData depositAdjustmentData) throws DaoException;

    DepositAdjustmentData get(String depositId, String adjustmentId) throws DaoException;

}
