package org.opencb.opencga.core.tools.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ApiOperation {
    String value();

    String notes() default "";

    String[] tags() default {""};

    Class<?> response() default Void.class;

    String responseContainer() default "";

    String responseReference() default "";

    String httpMethod() default "";

    /**
     * @deprecated
     */
    @Deprecated
    int position() default 0;

    String nickname() default "";

    String produces() default "";

    String consumes() default "";

    String protocols() default "";

    boolean hidden() default false;

    int code() default 200;

    boolean ignoreJsonView() default false;
}
