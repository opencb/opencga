package org.opencb.opencga.core.tools.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DataClass {

    /**
     * @return id of the field.
     */
    String id() default "";

    boolean managed() default false;

    boolean deprecated() default false;

    /**
     * @return Version when this field was added, eg. "2.1"
     */
    String since() default "";

    String description() default "";

}
