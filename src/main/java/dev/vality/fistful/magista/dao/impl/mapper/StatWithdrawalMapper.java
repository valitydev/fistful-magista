package dev.vality.fistful.magista.dao.impl.mapper;

import dev.vality.fistful.base.SubFailure;
import dev.vality.fistful.fistful_stat.*;
import dev.vality.fistful.magista.domain.enums.WithdrawalStatus;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.geck.common.util.TypeUtil;
import org.apache.logging.log4j.util.Strings;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Map;

import static dev.vality.fistful.magista.domain.tables.WithdrawalData.WITHDRAWAL_DATA;

public class StatWithdrawalMapper implements RowMapper<Map.Entry<Long, StatWithdrawal>> {

    @Override
    public Map.Entry<Long, StatWithdrawal> mapRow(ResultSet rs, int i) throws SQLException {
        StatWithdrawal statWithdrawal = new StatWithdrawal();
        statWithdrawal.setId(rs.getString(WITHDRAWAL_DATA.WITHDRAWAL_ID.getName()));
        statWithdrawal.setCreatedAt(
                TypeUtil.temporalToString(rs.getObject(WITHDRAWAL_DATA.CREATED_AT.getName(), LocalDateTime.class)));
        statWithdrawal.setIdentityId(rs.getString(WITHDRAWAL_DATA.IDENTITY_ID.getName()));
        statWithdrawal.setSourceId(rs.getString(WITHDRAWAL_DATA.WALLET_ID.getName()));
        statWithdrawal.setDestinationId(rs.getString(WITHDRAWAL_DATA.DESTINATION_ID.getName()));
        statWithdrawal.setAmount(rs.getLong(WITHDRAWAL_DATA.AMOUNT.getName()));
        statWithdrawal.setFee(rs.getLong(WITHDRAWAL_DATA.FEE.getName()));
        statWithdrawal.setCurrencySymbolicCode(rs.getString(WITHDRAWAL_DATA.CURRENCY_CODE.getName()));
        statWithdrawal.setExternalId(rs.getString(WITHDRAWAL_DATA.EXTERNAL_ID.getName()));
        WithdrawalStatus withdrawalStatus =
                TypeUtil.toEnumField(rs.getString(WITHDRAWAL_DATA.WITHDRAWAL_STATUS.getName()), WithdrawalStatus.class);
        switch (withdrawalStatus) {
            case pending:
                statWithdrawal
                        .setStatus(dev.vality.fistful.fistful_stat.WithdrawalStatus.pending(new WithdrawalPending()));
                break;
            case succeeded:
                statWithdrawal.setStatus(
                        dev.vality.fistful.fistful_stat.WithdrawalStatus.succeeded(new WithdrawalSucceeded()));
                break;
            case failed:
                var baseFailure = new dev.vality.fistful.base.Failure();
                baseFailure.setCode(rs.getString(WITHDRAWAL_DATA.ERROR_CODE.getName()));
                baseFailure.setReason(rs.getString(WITHDRAWAL_DATA.ERROR_REASON.getName()));
                String errorSubFailure = rs.getString(WITHDRAWAL_DATA.ERROR_SUB_FAILURE.getName());
                if (Strings.isNotEmpty(errorSubFailure)) {
                    baseFailure.setSub(new SubFailure(errorSubFailure));
                }
                WithdrawalFailed withdrawalFailed = new WithdrawalFailed();
                withdrawalFailed.setFailure(new Failure());
                withdrawalFailed.setBaseFailure(baseFailure);
                statWithdrawal.setStatus(dev.vality.fistful.fistful_stat.WithdrawalStatus.failed(withdrawalFailed));
                break;
            default:
                throw new NotFoundException(
                        String.format("Withdrawal status '%s' not found", withdrawalStatus.getLiteral()));
        }

        return new AbstractMap.SimpleEntry<>(rs.getLong(WITHDRAWAL_DATA.ID.getName()), statWithdrawal);
    }
}
