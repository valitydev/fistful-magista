package dev.vality.fistful.magista.dao.impl.mapper;

import dev.vality.fistful.fistful_stat.*;
import dev.vality.fistful.magista.exception.NotFoundException;
import dev.vality.geck.common.util.TypeUtil;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Map;

import static dev.vality.fistful.magista.domain.tables.SourceData.SOURCE_DATA;

public class StatSourceMapper implements RowMapper<Map.Entry<Long, StatSource>> {

    @Override
    public Map.Entry<Long, StatSource> mapRow(ResultSet rs, int i) throws SQLException {
        StatSource statSource = new StatSource();
        statSource.setId(rs.getString(SOURCE_DATA.SOURCE_ID.getName()));
        statSource.setName(rs.getString(SOURCE_DATA.NAME.getName()));
        statSource.setCreatedAt(
                TypeUtil.temporalToString(rs.getObject(SOURCE_DATA.CREATED_AT.getName(), LocalDateTime.class)));
        statSource.setIdentity(rs.getString(SOURCE_DATA.ACCOUNT_IDENTITY_ID.getName()));
        statSource.setResource(SourceResource.internal(new SourceResourceInternal()
                .setDetails(rs.getString(SOURCE_DATA.RESOURCE_INTERNAL_DETAILS.getName()))));
        statSource.setCurrencySymbolicCode(rs.getString(SOURCE_DATA.ACCOUNT_CURRENCY.getName()));
        statSource.setExternalId(rs.getString(SOURCE_DATA.EXTERNAL_ID.getName()));
        if (rs.getString(SOURCE_DATA.STATUS.getName()) != null) {
            statSource.setStatus(getStatus(rs.getString(SOURCE_DATA.STATUS.getName())));
        }

        return new AbstractMap.SimpleEntry<>(rs.getLong(SOURCE_DATA.ID.getName()), statSource);
    }

    private SourceStatus getStatus(String status) {
        var withdrawalStatus = TypeUtil.toEnumField(status,
                dev.vality.fistful.magista.domain.enums.SourceStatus.class);
        return switch (withdrawalStatus) {
            case authorized ->
                SourceStatus.authorized(new Authorized());
            case unauthorized ->
                SourceStatus.unauthorized(new Unauthorized());
            default -> throw new NotFoundException(
                        String.format("Withdrawal status '%s' not found", withdrawalStatus.getLiteral()));
        };
    }
}
