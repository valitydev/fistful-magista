package dev.vality.fistful.magista.dao.impl;

import com.zaxxer.hikari.HikariDataSource;
import dev.vality.fistful.fistful_stat.StatDeposit;
import dev.vality.fistful.fistful_stat.StatSource;
import dev.vality.fistful.fistful_stat.StatWithdrawal;
import dev.vality.fistful.magista.dao.SearchDao;
import dev.vality.fistful.magista.dao.impl.field.ConditionParameterSource;
import dev.vality.fistful.magista.dao.impl.mapper.StatDepositMapper;
import dev.vality.fistful.magista.dao.impl.mapper.StatSourceMapper;
import dev.vality.fistful.magista.dao.impl.mapper.StatWithdrawalMapper;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataStatus;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.query.impl.SourceFunction;
import dev.vality.fistful.magista.query.impl.WithdrawalFunction;
import dev.vality.fistful.magista.query.impl.parameters.DepositParameters;
import org.jooq.Operator;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static dev.vality.fistful.magista.domain.tables.DepositData.DEPOSIT_DATA;
import static dev.vality.fistful.magista.domain.tables.DepositRevertData.DEPOSIT_REVERT_DATA;
import static dev.vality.fistful.magista.domain.tables.SourceData.SOURCE_DATA;
import static dev.vality.fistful.magista.domain.tables.WithdrawalData.WITHDRAWAL_DATA;
import static org.jooq.Comparator.*;

@Component
public class SearchDaoImpl extends AbstractGenericDao implements SearchDao {

    private final StatWithdrawalMapper statWithdrawalMapper;
    private final StatSourceMapper statSourceMapper;
    private final StatDepositMapper statDepositMapper;

    public SearchDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        statWithdrawalMapper = new StatWithdrawalMapper();
        statSourceMapper = new StatSourceMapper();
        statDepositMapper = new StatDepositMapper();
    }

    @Override
    public Collection<Map.Entry<Long, StatWithdrawal>> getWithdrawals(
            WithdrawalFunction.WithdrawalParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException {
        Query query = getDslContext()
                .select()
                .from(WITHDRAWAL_DATA)
                .where(
                        appendDateTimeRangeConditions(
                                appendConditions(DSL.trueCondition(), Operator.AND,
                                        new ConditionParameterSource()
                                                .addValue(WITHDRAWAL_DATA.PARTY_ID,
                                                        Optional.ofNullable(parameters.getPartyId())
                                                                .map(UUID::fromString)
                                                                .orElse(null), EQUALS)
                                                .addValue(WITHDRAWAL_DATA.WALLET_ID, parameters.getWalletId(), EQUALS)
                                                .addValue(WITHDRAWAL_DATA.WITHDRAWAL_ID, parameters.getWithdrawalId(),
                                                        EQUALS)
                                                .addInConditionValue(
                                                        WITHDRAWAL_DATA.WITHDRAWAL_ID, parameters.getWithdrawalIds())
                                                .addValue(WITHDRAWAL_DATA.IDENTITY_ID, parameters.getIdentityId(),
                                                        EQUALS)
                                                .addValue(WITHDRAWAL_DATA.DESTINATION_ID, parameters.getDestinationId(),
                                                        EQUALS)
                                                .addValue(WITHDRAWAL_DATA.AMOUNT, parameters.getAmountFrom(), GREATER)
                                                .addValue(WITHDRAWAL_DATA.AMOUNT, parameters.getAmountTo(), LESS)
                                                .addValue(WITHDRAWAL_DATA.CURRENCY_CODE, parameters.getCurrencyCode(),
                                                        EQUALS)
                                                .addValue(WITHDRAWAL_DATA.WITHDRAWAL_STATUS, parameters.getStatus(),
                                                        EQUALS)
                                                .addValue(WITHDRAWAL_DATA.EXTERNAL_ID, parameters.getExternalId(),
                                                        EQUALS)
                                                .addValue(WITHDRAWAL_DATA.PROVIDER_ID,
                                                        parameters.getWithdrawalProviderId(),
                                                        EQUALS)
                                                .addValue(WITHDRAWAL_DATA.TERMINAL_ID,
                                                        parameters.getWithdrawalTerminalId(),
                                                        EQUALS)
                                                .addValue(WITHDRAWAL_DATA.ID, fromId, LESS))
                                        .and(appendConditions(DSL.noCondition(), Operator.OR,
                                                new ConditionParameterSource()
                                                        .addValue(WITHDRAWAL_DATA.ERROR_REASON,
                                                                parameters.getErrorMessage() != null
                                                                        ? "%" + parameters.getErrorMessage() + "%"
                                                                        : null,
                                                                LIKE)
                                                        .addValue(WITHDRAWAL_DATA.ERROR_CODE,
                                                                parameters.getErrorMessage() != null
                                                                        ? "%" + parameters.getErrorMessage() + "%"
                                                                        : null,
                                                                LIKE)
                                                        .addValue(WITHDRAWAL_DATA.ERROR_SUB_FAILURE,
                                                                parameters.getErrorMessage() != null
                                                                        ? "%" + parameters.getErrorMessage() + "%"
                                                                        : null,
                                                                LIKE))),
                                WITHDRAWAL_DATA.CREATED_AT,
                                fromTime,
                                toTime
                        )
                )
                .and(WITHDRAWAL_DATA.PARTY_ID.isNotNull())
                .and(WITHDRAWAL_DATA.IDENTITY_ID.isNotNull())
                .orderBy(WITHDRAWAL_DATA.ID.desc()).limit(limit);

        return fetch(query, statWithdrawalMapper);
    }

    @Override
    public Collection<Map.Entry<Long, StatDeposit>> getDeposits(
            DepositParameters parameters,
            LocalDateTime fromTime,
            LocalDateTime toTime,
            Long fromId,
            int limit
    ) throws DaoException {
        Query query = getDslContext()
                .select(DEPOSIT_DATA.ID, DEPOSIT_DATA.EVENT_ID, DEPOSIT_DATA.EVENT_CREATED_AT, DEPOSIT_DATA.DEPOSIT_ID,
                        DEPOSIT_DATA.EVENT_OCCURED_AT, DEPOSIT_DATA.EVENT_TYPE, DEPOSIT_DATA.WALLET_ID,
                        DEPOSIT_DATA.SOURCE_ID, DEPOSIT_DATA.AMOUNT, DEPOSIT_DATA.CURRENCY_CODE,
                        DEPOSIT_DATA.DEPOSIT_STATUS, DEPOSIT_DATA.DEPOSIT_TRANSFER_STATUS, DEPOSIT_DATA.FEE,
                        DEPOSIT_DATA.PROVIDER_FEE, DEPOSIT_DATA.PARTY_ID, DEPOSIT_DATA.IDENTITY_ID, DEPOSIT_DATA.WTIME,
                        DEPOSIT_DATA.CREATED_AT, DEPOSIT_DATA.DESCRIPTION,
                        DSL.sum(DEPOSIT_REVERT_DATA.AMOUNT).as("REVERT_AMOUNT"))
                .from(DEPOSIT_DATA.leftJoin(DEPOSIT_REVERT_DATA)
                        .on(DEPOSIT_DATA.PARTY_ID.eq(DEPOSIT_REVERT_DATA.PARTY_ID)
                                .and(DEPOSIT_DATA.WALLET_ID.eq(DEPOSIT_REVERT_DATA.WALLET_ID)
                                        .and(DEPOSIT_DATA.DEPOSIT_ID.eq(DEPOSIT_REVERT_DATA.DEPOSIT_ID))
                                        .and(DEPOSIT_REVERT_DATA.STATUS.eq(DepositRevertDataStatus.succeeded)))))
                .where(
                        appendDateTimeRangeConditions(
                                appendConditions(DSL.trueCondition(), Operator.AND,
                                        new ConditionParameterSource()
                                                .addValue(DEPOSIT_DATA.PARTY_ID, parameters.getPartyId(), EQUALS)
                                                .addValue(DEPOSIT_DATA.DEPOSIT_ID,
                                                        parameters.getDepositId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_DATA.IDENTITY_ID,
                                                        parameters.getIdentityId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_DATA.WALLET_ID, parameters.getWalletId().orElse(null),
                                                        EQUALS)
                                                .addValue(DEPOSIT_DATA.SOURCE_ID, parameters.getSourceId().orElse(null),
                                                        EQUALS)
                                                .addValue(DEPOSIT_DATA.AMOUNT, parameters.getAmountFrom().orElse(null),
                                                        GREATER_OR_EQUAL)
                                                .addValue(DEPOSIT_DATA.AMOUNT, parameters.getAmountTo().orElse(null),
                                                        LESS_OR_EQUAL)
                                                .addValue(DEPOSIT_DATA.CURRENCY_CODE,
                                                        parameters.getCurrencyCode().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_DATA.DEPOSIT_STATUS,
                                                        parameters.getStatus().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_DATA.ID, fromId, LESS)
                                ),
                                DEPOSIT_DATA.CREATED_AT,
                                fromTime,
                                toTime
                        )
                )
                .groupBy(DEPOSIT_DATA.ID, DEPOSIT_DATA.EVENT_ID, DEPOSIT_DATA.EVENT_CREATED_AT, DEPOSIT_DATA.DEPOSIT_ID,
                        DEPOSIT_DATA.EVENT_OCCURED_AT, DEPOSIT_DATA.EVENT_TYPE, DEPOSIT_DATA.WALLET_ID,
                        DEPOSIT_DATA.SOURCE_ID, DEPOSIT_DATA.AMOUNT, DEPOSIT_DATA.CURRENCY_CODE,
                        DEPOSIT_DATA.DEPOSIT_STATUS, DEPOSIT_DATA.DEPOSIT_TRANSFER_STATUS, DEPOSIT_DATA.FEE,
                        DEPOSIT_DATA.PROVIDER_FEE, DEPOSIT_DATA.PARTY_ID, DEPOSIT_DATA.IDENTITY_ID, DEPOSIT_DATA.WTIME,
                        DEPOSIT_DATA.CREATED_AT)
                .orderBy(DEPOSIT_DATA.ID.desc())
                .limit(limit);

        return fetch(query, statDepositMapper);
    }

    @Override
    public Collection<Map.Entry<Long, StatSource>> getSources(
            SourceFunction.SourceParameters parameters, LocalDateTime fromTime,
            LocalDateTime toTime, Long fromId, int limit)
            throws DaoException {
        Query query = getDslContext()
                .select()
                .from(SOURCE_DATA)
                .where(
                        appendDateTimeRangeConditions(
                                appendConditions(DSL.trueCondition(), Operator.AND,
                                        new ConditionParameterSource()
                                                .addValue(SOURCE_DATA.SOURCE_ID, parameters.getSourceId(), EQUALS)
                                                .addValue(SOURCE_DATA.ACCOUNT_IDENTITY_ID, parameters.getIdentityId(),
                                                        EQUALS)
                                                .addValue(SOURCE_DATA.ACCOUNT_CURRENCY, parameters.getCurrencyCode(),
                                                        EQUALS)
                                                .addValue(SOURCE_DATA.STATUS, parameters.getStatus(),
                                                        EQUALS)
                                                .addValue(SOURCE_DATA.EXTERNAL_ID, parameters.getExternalId(),
                                                        EQUALS)
                                                .addValue(SOURCE_DATA.ID, fromId, LESS)),
                                SOURCE_DATA.CREATED_AT,
                                fromTime,
                                toTime
                        )
                )
                .orderBy(SOURCE_DATA.ID.desc()).limit(limit);
        return fetch(query, statSourceMapper);
    }
}
