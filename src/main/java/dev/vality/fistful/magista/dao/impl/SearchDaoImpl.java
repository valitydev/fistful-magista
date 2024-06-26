package dev.vality.fistful.magista.dao.impl;

import com.zaxxer.hikari.HikariDataSource;
import dev.vality.fistful.fistful_stat.*;
import dev.vality.fistful.magista.dao.SearchDao;
import dev.vality.fistful.magista.dao.impl.field.ConditionParameterSource;
import dev.vality.fistful.magista.dao.impl.mapper.*;
import dev.vality.fistful.magista.domain.enums.DepositRevertDataStatus;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.query.impl.SourceFunction;
import dev.vality.fistful.magista.query.impl.WalletFunction;
import dev.vality.fistful.magista.query.impl.WithdrawalFunction;
import dev.vality.fistful.magista.query.impl.parameters.DepositAdjustmentParameters;
import dev.vality.fistful.magista.query.impl.parameters.DepositParameters;
import dev.vality.fistful.magista.query.impl.parameters.DepositRevertParameters;
import dev.vality.fistful.magista.query.impl.parameters.IdentityParameters;
import dev.vality.geck.common.util.TypeUtil;
import org.jooq.Operator;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static dev.vality.fistful.magista.domain.tables.ChallengeData.CHALLENGE_DATA;
import static dev.vality.fistful.magista.domain.tables.DepositAdjustmentData.DEPOSIT_ADJUSTMENT_DATA;
import static dev.vality.fistful.magista.domain.tables.DepositData.DEPOSIT_DATA;
import static dev.vality.fistful.magista.domain.tables.DepositRevertData.DEPOSIT_REVERT_DATA;
import static dev.vality.fistful.magista.domain.tables.IdentityData.IDENTITY_DATA;
import static dev.vality.fistful.magista.domain.tables.SourceData.SOURCE_DATA;
import static dev.vality.fistful.magista.domain.tables.WalletData.WALLET_DATA;
import static dev.vality.fistful.magista.domain.tables.WithdrawalData.WITHDRAWAL_DATA;
import static org.jooq.Comparator.*;

@Component
public class SearchDaoImpl extends AbstractGenericDao implements SearchDao {

    private final StatWalletMapper statWalletMapper;
    private final StatWithdrawalMapper statWithdrawalMapper;
    private final StatSourceMapper statSourceMapper;
    private final StatDepositMapper statDepositMapper;
    private final StatIdentityMapper statIdentityMapper;
    private final StatDepositRevertMapper statDepositRevertMapper;
    private final StatDepositAdjustmentMapper statDepositAdjustmentMapper;

    public SearchDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        statWalletMapper = new StatWalletMapper();
        statWithdrawalMapper = new StatWithdrawalMapper();
        statSourceMapper = new StatSourceMapper();
        statDepositMapper = new StatDepositMapper();
        statIdentityMapper = new StatIdentityMapper();
        statDepositRevertMapper = new StatDepositRevertMapper();
        statDepositAdjustmentMapper = new StatDepositAdjustmentMapper();
    }

    @Override
    public Collection<Map.Entry<Long, StatWallet>> getWallets(
            WalletFunction.WalletParameters parameters,
            Optional<LocalDateTime> fromDate,
            int limit
    ) throws DaoException {
        Query query = getDslContext()
                .select()
                .from(WALLET_DATA)
                .where(
                        appendConditions(DSL.trueCondition(), Operator.AND, new ConditionParameterSource()
                                .addInConditionValue(WALLET_DATA.WALLET_ID, parameters.getWalletIds())
                                .addValue(WALLET_DATA.PARTY_ID, Optional.ofNullable(parameters.getPartyId())
                                        .map(UUID::fromString)
                                        .orElse(null), EQUALS)
                                .addValue(WALLET_DATA.IDENTITY_ID, parameters.getIdentityId(), EQUALS)
                                .addValue(WALLET_DATA.CURRENCY_CODE, parameters.getCurrencyCode(), EQUALS)
                                .addValue(WALLET_DATA.CREATED_AT, fromDate.orElse(null), LESS))
                )
                .and(WALLET_DATA.PARTY_ID.isNotNull())
                .and(WALLET_DATA.IDENTITY_ID.isNotNull())
                .orderBy(WALLET_DATA.CREATED_AT.desc())
                .limit(limit);

        return fetch(query, statWalletMapper);
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
    public Collection<Map.Entry<Long, StatIdentity>> getIdentities(IdentityParameters parameters,
                                                                   LocalDateTime fromTime, LocalDateTime toTime,
                                                                   Long fromId, int limit) throws DaoException {
        Query query = getDslContext()
                .select()
                .from(IDENTITY_DATA.leftJoin(CHALLENGE_DATA)
                        .on(IDENTITY_DATA.IDENTITY_ID.eq(CHALLENGE_DATA.IDENTITY_ID)))
                .where(
                        appendDateTimeRangeConditions(
                                appendConditions(DSL.trueCondition(), Operator.AND,
                                        new ConditionParameterSource()
                                                .addValue(IDENTITY_DATA.PARTY_ID, parameters.getPartyId(), EQUALS)
                                                .addValue(IDENTITY_DATA.PARTY_CONTRACT_ID,
                                                        parameters.getPartyContractId().orElse(null), EQUALS)
                                                .addValue(IDENTITY_DATA.IDENTITY_ID,
                                                        parameters.getIdentityId().orElse(null), EQUALS)
                                                .addValue(IDENTITY_DATA.IDENTITY_PROVIDER_ID,
                                                        parameters.getIdentityProviderId().orElse(null), EQUALS)
                                                .addValue(IDENTITY_DATA.IDENTITY_EFFECTIVE_CHALLENGE_ID,
                                                        parameters.getIdentityEffectiveChallengeId().orElse(null),
                                                        EQUALS)
                                                .addValue(IDENTITY_DATA.IDENTITY_LEVEL_ID,
                                                        parameters.getIdentityLevelId().orElse(null), EQUALS)
                                                .addValue(IDENTITY_DATA.ID, fromId, LESS)
                                                .addValue(CHALLENGE_DATA.CHALLENGE_ID,
                                                        parameters.getChallengeId().orElse(null), EQUALS)
                                                .addValue(CHALLENGE_DATA.CHALLENGE_CLASS_ID,
                                                        parameters.getChallengeClassId().orElse(null), EQUALS)
                                                .addValue(CHALLENGE_DATA.CHALLENGE_STATUS,
                                                        parameters.getChallengeStatus().orElse(null), EQUALS)
                                                .addValue(CHALLENGE_DATA.CHALLENGE_RESOLUTION,
                                                        parameters.getChallengeResolution().orElse(null), EQUALS)
                                                .addValue(CHALLENGE_DATA.CHALLENGE_VALID_UNTIL,
                                                        TypeUtil.toLocalDateTime(parameters.getChallengeValidUntil()),
                                                        GREATER_OR_EQUAL)

                                ),
                                IDENTITY_DATA.CREATED_AT,
                                fromTime,
                                toTime
                        )
                )
                .orderBy(IDENTITY_DATA.ID.desc())
                .limit(limit);

        return fetch(query, statIdentityMapper);
    }

    @Override
    public Collection<Map.Entry<Long, StatDepositRevert>> getDepositReverts(DepositRevertParameters parameters,
                                                                            LocalDateTime fromTime,
                                                                            LocalDateTime toTime, Long fromId,
                                                                            int limit) throws DaoException {
        Query query = getDslContext()
                .select()
                .from(DEPOSIT_REVERT_DATA)
                .where(
                        appendDateTimeRangeConditions(
                                appendConditions(DSL.trueCondition(), Operator.AND,
                                        new ConditionParameterSource()
                                                .addValue(DEPOSIT_REVERT_DATA.PARTY_ID, parameters.getPartyId(), EQUALS)
                                                .addValue(DEPOSIT_REVERT_DATA.IDENTITY_ID,
                                                        parameters.getIdentityId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_REVERT_DATA.SOURCE_ID,
                                                        parameters.getSourceId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_REVERT_DATA.WALLET_ID,
                                                        parameters.getWalletId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_REVERT_DATA.DEPOSIT_ID,
                                                        parameters.getDepositId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_REVERT_DATA.REVERT_ID,
                                                        parameters.getRevertId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_REVERT_DATA.AMOUNT,
                                                        parameters.getAmountFrom().orElse(null), GREATER_OR_EQUAL)
                                                .addValue(DEPOSIT_REVERT_DATA.AMOUNT,
                                                        parameters.getAmountTo().orElse(null), LESS_OR_EQUAL)
                                                .addValue(DEPOSIT_REVERT_DATA.CURRENCY_CODE,
                                                        parameters.getCurrencyCode().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_REVERT_DATA.STATUS,
                                                        parameters.getStatus().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_REVERT_DATA.ID, fromId, LESS)
                                ),
                                DEPOSIT_REVERT_DATA.CREATED_AT,
                                fromTime,
                                toTime
                        )
                )
                .orderBy(DEPOSIT_REVERT_DATA.ID.desc())
                .limit(limit);

        return fetch(query, statDepositRevertMapper);
    }

    @Override
    public Collection<Map.Entry<Long, StatDepositAdjustment>> getDepositAdjustments(
            DepositAdjustmentParameters parameters, LocalDateTime fromTime, LocalDateTime toTime, Long fromId,
            int limit) throws DaoException {
        Query query = getDslContext()
                .select()
                .from(DEPOSIT_ADJUSTMENT_DATA)
                .where(
                        appendDateTimeRangeConditions(
                                appendConditions(DSL.trueCondition(), Operator.AND,
                                        new ConditionParameterSource()
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.PARTY_ID, parameters.getPartyId(),
                                                        EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.IDENTITY_ID,
                                                        parameters.getIdentityId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.SOURCE_ID,
                                                        parameters.getSourceId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.WALLET_ID,
                                                        parameters.getWalletId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.DEPOSIT_ID,
                                                        parameters.getDepositId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.ADJUSTMENT_ID,
                                                        parameters.getAdjustmentId().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.AMOUNT,
                                                        parameters.getAmountFrom().orElse(null), GREATER_OR_EQUAL)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.AMOUNT,
                                                        parameters.getAmountTo().orElse(null), LESS_OR_EQUAL)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.CURRENCY_CODE,
                                                        parameters.getCurrencyCode().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.STATUS,
                                                        parameters.getDepositAdjustmentStatus().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.DEPOSIT_STATUS,
                                                        parameters.getDepositStatus().orElse(null), EQUALS)
                                                .addValue(DEPOSIT_ADJUSTMENT_DATA.ID, fromId, LESS)
                                ),
                                DEPOSIT_ADJUSTMENT_DATA.CREATED_AT,
                                fromTime,
                                toTime
                        )
                )
                .orderBy(DEPOSIT_ADJUSTMENT_DATA.ID.desc())
                .limit(limit);

        return fetch(query, statDepositAdjustmentMapper);
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
