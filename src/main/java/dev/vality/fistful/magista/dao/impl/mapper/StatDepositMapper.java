package dev.vality.fistful.magista.dao.impl.mapper;

import dev.vality.fistful.base.Failure;
import dev.vality.fistful.fistful_stat.DepositFailed;
import dev.vality.fistful.fistful_stat.DepositPending;
import dev.vality.fistful.fistful_stat.DepositSucceeded;
import dev.vality.fistful.fistful_stat.StatDeposit;
import dev.vality.fistful.magista.domain.enums.DepositStatus;
import dev.vality.geck.common.util.TypeUtil;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

import static dev.vality.fistful.magista.domain.tables.DepositData.DEPOSIT_DATA;

public class StatDepositMapper implements RowMapper<Map.Entry<Long, StatDeposit>> {

    @Override
    public Map.Entry<Long, StatDeposit> mapRow(ResultSet rs, int i) throws SQLException {
        StatDeposit deposit = new StatDeposit();
        deposit.setId(rs.getString(DEPOSIT_DATA.DEPOSIT_ID.getName()));
        deposit.setPartyId(rs.getString(DEPOSIT_DATA.PARTY_ID.getName()));
        deposit.setCreatedAt(
                TypeUtil.temporalToString(rs.getObject(DEPOSIT_DATA.CREATED_AT.getName(), LocalDateTime.class)));
        deposit.setDestinationId(rs.getString(DEPOSIT_DATA.WALLET_ID.getName()));
        deposit.setSourceId(rs.getString(DEPOSIT_DATA.SOURCE_ID.getName()));
        deposit.setAmount(rs.getLong(DEPOSIT_DATA.AMOUNT.getName()));
        deposit.setFee(rs.getLong(DEPOSIT_DATA.FEE.getName()));
        deposit.setCurrencySymbolicCode(rs.getString(DEPOSIT_DATA.CURRENCY_CODE.getName()));
        DepositStatus depositStatus =
                TypeUtil.toEnumField(rs.getString(DEPOSIT_DATA.DEPOSIT_STATUS.getName()), DepositStatus.class);
        String statusFailCode = rs.getString(DEPOSIT_DATA.DEPOSIT_STATUS_FAIL_CODE.getName());
        if (statusFailCode == null || statusFailCode.isEmpty()) {
            statusFailCode = "unknown";
        }
        deposit.setStatus(getDepositStatus(depositStatus, statusFailCode));
        deposit.setDescription(rs.getString(DEPOSIT_DATA.DESCRIPTION.getName()));
        return new SimpleEntry<>(rs.getLong(DEPOSIT_DATA.ID.getName()), deposit);
    }

    private dev.vality.fistful.fistful_stat.DepositStatus getDepositStatus(DepositStatus depositStatus, String failCode) {
        return switch (depositStatus) {
            case succeeded -> dev.vality.fistful.fistful_stat.DepositStatus.succeeded(new DepositSucceeded());
            case pending -> dev.vality.fistful.fistful_stat.DepositStatus.pending(new DepositPending());
            case failed ->
                    dev.vality.fistful.fistful_stat.DepositStatus.failed(new DepositFailed(new Failure(failCode)));
        };
    }
}
