package dev.vality.fistful.magista.dao.impl.mapper;

import dev.vality.fistful.fistful_stat.StatWallet;
import dev.vality.geck.common.util.TypeUtil;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Map;

import static dev.vality.fistful.magista.domain.tables.WalletData.WALLET_DATA;

public class StatWalletMapper implements RowMapper<Map.Entry<Long, StatWallet>> {

    @Override
    public Map.Entry<Long, StatWallet> mapRow(ResultSet rs, int i) throws SQLException {
        StatWallet statWallet = new StatWallet();
        statWallet.setId(rs.getString(WALLET_DATA.WALLET_ID.getName()));
        statWallet.setIdentityId(rs.getString(WALLET_DATA.IDENTITY_ID.getName()));
        statWallet.setName(rs.getString(WALLET_DATA.WALLET_NAME.getName()));
        statWallet.setCreatedAt(
                TypeUtil.temporalToString(rs.getObject(WALLET_DATA.CREATED_AT.getName(), LocalDateTime.class)));
        statWallet.setCurrencySymbolicCode(rs.getString(WALLET_DATA.CURRENCY_CODE.getName()));
        return new AbstractMap.SimpleEntry<>(rs.getLong(WALLET_DATA.ID.getName()), statWallet);
    }
}
