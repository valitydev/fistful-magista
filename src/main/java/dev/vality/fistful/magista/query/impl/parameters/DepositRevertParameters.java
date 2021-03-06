package dev.vality.fistful.magista.query.impl.parameters;

import dev.vality.fistful.magista.domain.enums.DepositRevertDataStatus;
import dev.vality.magista.dsl.PagedBaseFunction;
import dev.vality.magista.dsl.QueryParameters;

import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dev.vality.fistful.magista.query.impl.Parameters.*;

public class DepositRevertParameters extends PagedBaseFunction.PagedBaseParameters {

    private static final Map<String, DepositRevertDataStatus> statusesMap = getDepositRevertStatusMap();

    public DepositRevertParameters(Map<String, Object> parameters, QueryParameters derivedParameters) {
        super(parameters, derivedParameters);
    }

    public DepositRevertParameters(QueryParameters parameters, QueryParameters derivedParameters) {
        super(parameters, derivedParameters);
    }

    public UUID getPartyId() {
        return Optional.ofNullable(getStringParameter(PARTY_ID_PARAM, false))
                .map(UUID::fromString)
                .orElse(null);
    }

    public Optional<String> getIdentityId() {
        return Optional.ofNullable(getStringParameter(IDENTITY_ID_PARAM, false));
    }

    public Optional<String> getSourceId() {
        return Optional.ofNullable(getStringParameter(SOURCE_ID_PARAM, false));
    }

    public Optional<String> getWalletId() {
        return Optional.ofNullable(getStringParameter(WALLET_ID_PARAM, false));
    }

    public Optional<String> getDepositId() {
        return Optional.ofNullable(getStringParameter(DEPOSIT_ID_PARAM, false));
    }

    public Optional<String> getRevertId() {
        return Optional.ofNullable(getStringParameter(REVERT_ID_PARAM, false));
    }

    public Optional<Long> getAmountFrom() {
        return Optional.ofNullable(getLongParameter(AMOUNT_FROM_PARAM, false));
    }

    public Optional<Long> getAmountTo() {
        return Optional.ofNullable(getLongParameter(AMOUNT_TO_PARAM, false));
    }

    public Optional<String> getCurrencyCode() {
        return Optional.ofNullable(getStringParameter(CURRENCY_CODE_PARAM, false));
    }

    public Optional<DepositRevertDataStatus> getStatus() {
        String status = getStringParameter(STATUS_PARAM, false);
        if (status != null && statusesMap.keySet().contains(status)) {
            return Optional.of(statusesMap.get(status));
        }
        return Optional.empty();
    }

    public TemporalAccessor getFromTime() {
        return getTimeParameter(FROM_TIME_PARAM, false);
    }

    public TemporalAccessor getToTime() {
        return getTimeParameter(TO_TIME_PARAM, false);
    }

    private static Map<String, DepositRevertDataStatus> getDepositRevertStatusMap() {
        return Collections.unmodifiableMap(
                Stream.of(
                        new AbstractMap.SimpleEntry<>("Pending", DepositRevertDataStatus.pending),
                        new AbstractMap.SimpleEntry<>("Succeeded", DepositRevertDataStatus.succeeded),
                        new AbstractMap.SimpleEntry<>("Failed", DepositRevertDataStatus.failed)
                )
                        .collect(
                                Collectors.toMap(
                                        AbstractMap.SimpleEntry::getKey,
                                        AbstractMap.SimpleEntry::getValue
                                )
                        )
        );
    }
}
