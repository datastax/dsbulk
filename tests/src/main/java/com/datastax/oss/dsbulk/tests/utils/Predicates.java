package com.datastax.oss.dsbulk.tests.utils;

import com.datastax.oss.driver.shaded.guava.common.base.Functions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class Predicates {

    public static Predicate<String> jsonWithField(String fieldName, String fieldValue) {
        return jsonWithField(fieldName, fieldValue, Optional.empty());
    }

    public static Predicate<String> jsonWithField(String fieldName, String fieldValue, Optional<Function<String,String>> transform) {
        return new Predicate<String>() {
            public boolean test(String arg) {

                try {

                    JsonNode argJson = new ObjectMapper().readTree(arg);
                    if (!argJson.has(fieldName))
                            return false;
                    return transform.orElse(Functions.identity()).apply(argJson.get(fieldName).toString()).equals(fieldValue);
                }
                catch (Exception e) {
                    return false;
                }
            }
        };
    }

}
