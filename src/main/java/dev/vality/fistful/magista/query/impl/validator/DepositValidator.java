package dev.vality.fistful.magista.query.impl.validator;

import dev.vality.fistful.magista.query.impl.parameters.DepositParameters;
import dev.vality.magista.dsl.PagedBaseFunction;
import dev.vality.magista.dsl.QueryParameters;

import static dev.vality.fistful.magista.query.impl.Parameters.STATUS_PARAM;

public class DepositValidator extends PagedBaseFunction.PagedBaseValidator {

    @Override
    public void validateParameters(QueryParameters parameters) throws IllegalArgumentException {
        super.validateParameters(parameters);
        DepositParameters depositParameters = super.checkParamsType(parameters, DepositParameters.class);

        //time
        if (depositParameters.getFromTime() != null && depositParameters.getToTime() != null) {
            validateTimePeriod(depositParameters.getFromTime(), depositParameters.getToTime());
        }

        //status
        String stringStatus = parameters.getStringParameter(STATUS_PARAM, false);
        if (stringStatus != null && !depositParameters.getStatus().isPresent()) {
            throw new IllegalArgumentException("Unknown deposit status: " + stringStatus);
        }
    }
}
