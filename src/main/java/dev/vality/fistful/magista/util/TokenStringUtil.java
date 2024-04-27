package dev.vality.fistful.magista.util;

import dev.vality.magista.dsl.QueryParameters;
import lombok.experimental.UtilityClass;

import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.*;


@UtilityClass
public class TokenStringUtil {
    public static Optional<String> extractIdValue(String token) {
        if (isBlank(token)) {
            return Optional.empty();
        }
        var params = token.split(";");
        return Optional.of(params[2]);
    }

    public static String buildToken(QueryParameters queryParameters, String id) {
        return "%d;%d;%s".formatted(queryParameters.hashCode(), id.hashCode(), id);
    }

    public static void validateToken(QueryParameters queryParameters, String token) {
        if (isNoneBlank(token)) {
            var params = token.split(";");
            if (params.length != 3) {
                throw new IllegalArgumentException("Invalid token");
            }
            int queryHashCode = Integer.parseInt(params[0]);
            int idHashCode = Integer.parseInt(params[1]);
            if (queryParameters.hashCode() != queryHashCode || params[2].hashCode() != idHashCode) {
                throw new IllegalArgumentException("Invalid token");
            }
        }
    }
}
