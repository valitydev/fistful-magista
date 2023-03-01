package dev.vality.fistful.magista.dao;

import dev.vality.fistful.fistful_stat.*;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.query.impl.SourceFunction;
import dev.vality.fistful.magista.query.impl.WalletFunction;
import dev.vality.fistful.magista.query.impl.WithdrawalFunction;
import dev.vality.fistful.magista.query.impl.parameters.DepositAdjustmentParameters;
import dev.vality.fistful.magista.query.impl.parameters.DepositParameters;
import dev.vality.fistful.magista.query.impl.parameters.DepositRevertParameters;
import dev.vality.fistful.magista.query.impl.parameters.IdentityParameters;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public interface SearchDao {

    Collection<Map.Entry<Long, StatWallet>> getWallets(
            WalletFunction.WalletParameters parameters,
            Optional<Long> fromId,
            int limit
    ) throws DaoException;

    Collection<Map.Entry<Long, StatWithdrawal>> getWithdrawals(
            WithdrawalFunction.WithdrawalParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException;

    Collection<Map.Entry<Long, StatDeposit>> getDeposits(
            DepositParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException;

    Collection<Map.Entry<Long, StatIdentity>> getIdentities(
            IdentityParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException;

    Collection<Map.Entry<Long, StatDepositRevert>> getDepositReverts(
            DepositRevertParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException;

    Collection<Map.Entry<Long, StatDepositAdjustment>> getDepositAdjustments(
            DepositAdjustmentParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException;

    Collection<Map.Entry<Long, StatSource>> getSources(
            SourceFunction.SourceParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException;

}
