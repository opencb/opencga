package org.opencb.opencga.core.tools.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ApiImplicitParam {
    String name() default "";

    String value() default "";

    String defaultValue() default "";

    String allowableValues() default "";

    boolean required() default false;

    String access() default "";

    boolean allowMultiple() default false;

    String dataType() default "";

    Class<?> dataTypeClass() default Void.class;

    String paramType() default "";

    String example() default "";

    String type() default "";

    String format() default "";

    boolean allowEmptyValue() default false;

    boolean readOnly() default false;

    String collectionFormat() default "";
}