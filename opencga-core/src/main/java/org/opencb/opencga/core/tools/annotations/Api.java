package org.opencb.opencga.core.tools.annotations;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Api {
    String value() default "";

    String[] tags() default {""};

    String description() default "";

    /**
     * @deprecated
     */
    @Deprecated
    String basePath() default "";

    /**
     * @deprecated
     */
    @Deprecated
    int position() default 0;

    String produces() default "";

    String consumes() default "";

    String protocols() default "";

    boolean hidden() default false;
}