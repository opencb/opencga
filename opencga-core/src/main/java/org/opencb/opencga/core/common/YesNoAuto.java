package org.opencb.opencga.core.common;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public enum YesNoAuto {

    YES,
    NO,
    AUTO;


    public static YesNoAuto parse(Map<String, ?> map, String key) {
        Object value = map.get(key);
        if (value == null ) {
            return AUTO;
        } else {
            return parse(value);
        }
    }

    public static YesNoAuto parse(Object value) {
        if (value == null) {
            return AUTO;
        }
        if (value instanceof YesNoAuto) {
            return ((YesNoAuto) value);
        }
        return parse(value.toString());
    }

    private static YesNoAuto parse(String value) {
        if (StringUtils.isBlank(value)) {
            return AUTO;
        }

        switch (value.toLowerCase()) {
            case "auto":
                return AUTO;
            case "yes":
            case "true":
                return YES;
            case "no":
            case "false":
                return NO;
            default:
                throw new IllegalArgumentException("Unknown option " + value);
        }
    }

    public Boolean booleanValue() {
        switch (this) {
            case YES:
                return true;
            case NO:
                return false;
            case AUTO:
                return null;
            default:
                throw new IllegalArgumentException("Unknown option " + this);
        }
    }
}
