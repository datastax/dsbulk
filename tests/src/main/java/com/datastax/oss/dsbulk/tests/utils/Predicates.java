package com.datastax.oss.dsbulk.tests.utils;

import com.datastax.oss.driver.shaded.guava.common.base.Functions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class Predicates {

    public static Predicate<String> jsonStringWithField(String fieldName, String fieldValue) {
        return jsonStringWithField(fieldName, fieldValue, Optional.empty());
    }

    public static Predicate<String> jsonStringWithField(String fieldName, String fieldValue, Optional<Function<String,String>> transform) {
        return arg -> {

            try {

                JsonNode argJson = new ObjectMapper().readTree(arg);
                if (!argJson.has(fieldName))
                        return false;
                return transform.orElse(Functions.identity()).apply(argJson.get(fieldName).toString()).equals(fieldValue);
            }
            catch (Exception e) {
                return false;
            }
        };
    }

    public static Predicate<JsonNode> jsonNodeWithField(String fieldName, String fieldValue) {
        return arg -> {

            try {

                return arg.get(fieldName).toString().equals(fieldValue);
            }
            catch (Exception e) {
                return false;
            }
        };
    }
}
