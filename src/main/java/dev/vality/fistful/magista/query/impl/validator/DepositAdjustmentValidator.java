package dev.vality.fistful.magista.query.impl.validator;

import dev.vality.fistful.magista.query.impl.parameters.DepositAdjustmentParameters;
import dev.vality.magista.dsl.PagedBaseFunction;
import dev.vality.magista.dsl.QueryParameters;

import static dev.vality.fistful.magista.query.impl.Parameters.STATUS_PARAM;

public class DepositAdjustmentValidator extends PagedBaseFunction.PagedBaseValidator {

    @Override
    public void validateParameters(QueryParameters queryParameters) throws IllegalArgumentException {
        super.validateParameters(queryParameters);
        DepositAdjustmentParameters parameters =
                super.checkParamsType(queryParameters, DepositAdjustmentParameters.class);

        //time
        if (parameters.getFromTime() != null && parameters.getToTime() != null) {
            validateTimePeriod(parameters.getFromTime(), parameters.getToTime());
        }

        //status
        String stringStatus = queryParameters.getStringParameter(STATUS_PARAM, false);
        if (stringStatus != null && parameters.getDepositAdjustmentStatus().isEmpty()) {
            throw new IllegalArgumentException("Unknown deposit adjustment status: " + stringStatus);
        }
    }
}
