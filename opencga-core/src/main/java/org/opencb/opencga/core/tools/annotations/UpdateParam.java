package org.opencb.opencga.core.tools.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface UpdateParam {

    /**
     * @return enum values QUERY, BODY, PATH.
     */
    ParamType type();

    /**
     * @return default value of the field.
     */
    String defaultValue() default "";

    /**
     * @return List of possible allowed values.
     */
    String allowableValues() default "";

    /**
     * @return If field is CLI required or not.
     */
    boolean required() default false;
}
