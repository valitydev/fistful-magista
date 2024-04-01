package dev.vality.fistful.magista.util;

import dev.vality.magista.dsl.QueryParameters;
import org.junit.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    @Test(expected = IllegalArgumentException.class)
    public void badQueryParametersTest() {
        var queryParam = new QueryParameters(Map.of("test_param", "test_value"), null);
        var id = "test1";
        var token = TokenStringUtil.buildToken(queryParam, id);

        var otherQueryParam = new QueryParameters(Map.of(), null);
        TokenStringUtil.validateToken(otherQueryParam, token);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidTokenTest1() {
        var otherQueryParam = new QueryParameters(Map.of(), null);
        TokenStringUtil.validateToken(otherQueryParam, "1;1;1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidTokenTest2() {
        var otherQueryParam = new QueryParameters(Map.of(), null);
        TokenStringUtil.validateToken(otherQueryParam, "1;1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidTokenMaxValueTest() {
        var otherQueryParam = new QueryParameters(Map.of(), null);
        TokenStringUtil.validateToken(otherQueryParam, "3147483647;3147483647;1");
    }


}