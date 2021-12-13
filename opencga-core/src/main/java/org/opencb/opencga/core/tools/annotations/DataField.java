package org.opencb.opencga.core.tools.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface DataField {

    /**
     * @return id of the field.
     */
    String id() default "";

    @Deprecated
    String name() default "";

    String[] alias() default {};

    boolean required() default false;

    boolean indexed() default false;

    boolean managed() default false;

    boolean immutable() default false;

    boolean unique() default false;

    boolean deprecated() default false;

    String defaultValue() default "";

    /**
     * @return Version when this field was added, eg. "2.1"
     */
    String since() default "";

    String[] dependsOn() default {};

    String description() default "";

//    @Deprecated
//    Class<?> dataTypeClass() default Void.class;
//
//    @Deprecated
//    UpdateParam update() default @UpdateParam;
//
//    @Deprecated
//    CreateParam create() default @CreateParam;
//
//    @Deprecated
//    SearchParam search() default @SearchParam;
}
