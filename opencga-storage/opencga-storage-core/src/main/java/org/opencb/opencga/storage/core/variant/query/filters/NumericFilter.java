package org.opencb.opencga.storage.core.variant.query.filters;

import java.util.function.Predicate;

public final class NumericFilter {

    public static Predicate<Float> parse(String operator, float value) {
        switch (operator) {
            case "<":
                return i -> i < value;
            case "<=":
                return i -> i <= value;
            case ">":
                return i -> i > value;
            case ">=":
                return i -> i >= value;
            case "":
            case "=":
            case "==":
                return i -> i == value;
            case "!=":
            case "!":
                return i -> i != value;
            default:
                throw new IllegalArgumentException("Unknown numeric operator '" + operator + "'");
        }
    }

}
