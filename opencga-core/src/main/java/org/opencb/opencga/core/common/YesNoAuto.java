package org.opencb.opencga.core.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public enum YesNoAuto {

    YES,
    NO,
    AUTO;


    public static YesNoAuto parse(Map<String, ?> map, String key) {
        if (map == null ) {
            return AUTO;
        }
        return parse(map.get(key));
    }

    @JsonCreator
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

    public boolean booleanValue(boolean defaultValue) {
        return booleanValue(Boolean.valueOf(defaultValue));
    }

    public Boolean booleanValue() {
        return booleanValue(null);
    }

    private Boolean booleanValue(Boolean defaultValue) {
        switch (this) {
            case YES:
                return true;
            case NO:
                return false;
            case AUTO:
                return defaultValue;
            default:
                throw new IllegalArgumentException("Unknown option " + this);
        }
    }
}
