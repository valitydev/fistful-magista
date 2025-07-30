package dev.vality.fistful.magista.dao;

import dev.vality.fistful.fistful_stat.StatDeposit;
import dev.vality.fistful.fistful_stat.StatSource;
import dev.vality.fistful.fistful_stat.StatWithdrawal;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.query.impl.SourceFunction;
import dev.vality.fistful.magista.query.impl.WithdrawalFunction;
import dev.vality.fistful.magista.query.impl.parameters.DepositParameters;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;

public interface SearchDao {

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

    Collection<Map.Entry<Long, StatSource>> getSources(
            SourceFunction.SourceParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException;

}
