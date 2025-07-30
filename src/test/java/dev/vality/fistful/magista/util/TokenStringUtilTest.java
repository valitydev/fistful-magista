package dev.vality.fistful.magista.util;

import dev.vality.magista.dsl.QueryParameters;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TokenStringUtilTest {

    @Test
    public void tokenTest() {
        var queryParam = new QueryParameters(Map.of("test_param", "test_value"), null);
        var id = "test1";
        var token = TokenStringUtil.buildToken(queryParam, id);
        TokenStringUtil.validateToken(queryParam, token);

        var extractId = TokenStringUtil.extractIdValue(token);
        assertEquals(id, extractId.get());
    }

    @Test
    public void badQueryParametersTest() {
        var queryParam = new QueryParameters(Map.of("test_param", "test_value"), null);
        var id = "test1";
        var token = TokenStringUtil.buildToken(queryParam, id);

        var otherQueryParam = new QueryParameters(Map.of(), null);
        assertThrows(IllegalArgumentException.class, () -> {
            TokenStringUtil.validateToken(otherQueryParam, token);
        });
    }

    @Test
    public void invalidQueryHashTest() {
        var queryParameters = new QueryParameters(Map.of(), null);
        assertThrows(IllegalArgumentException.class, () -> {
            TokenStringUtil.validateToken(queryParameters, "1;" + "1".hashCode() + ";1");
        });
    }

    @Test
    public void invalidIdHashTest() {
        var queryParameters = new QueryParameters(Map.of(), null);
        assertThrows(IllegalArgumentException.class, () -> {
            TokenStringUtil.validateToken(queryParameters, queryParameters.hashCode() + ";1;1");
        });
    }

    @Test
    public void invalidTokenTest() {
        var queryParameters = new QueryParameters(Map.of(), null);
        assertThrows(IllegalArgumentException.class, () -> {
            TokenStringUtil.validateToken(queryParameters, "1;1");
        });
    }

    @Test
    public void invalidTokenMaxValueTest() {
        var queryParameters = new QueryParameters(Map.of(), null);
        assertThrows(IllegalArgumentException.class, () -> {
            TokenStringUtil.validateToken(queryParameters, "3147483647;3147483647;1");
        });
    }
}