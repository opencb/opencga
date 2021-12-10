package org.opencb.opencga.core.tools.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface DataModel {

    /**
     * @return id of the field.
     */
    String id() default "";

    @Deprecated
    String name() default "";

    String defaultValue() default "";

    String description() default "";

    /**
     * @return Class of field.
     */
    @Deprecated
    Class<?> dataTypeClass() default Void.class;

    boolean required() default false;

    boolean indexed() default false;

    boolean managed() default false;

    boolean updatable() default true;

    boolean unique() default false;

    boolean deprecated() default false;

    /**
     * @return Version when this field was added, eg. "2.1"
     */
    String since() default "";

    String[] dependsOn() default {};

//    @Deprecated
//    FieldScope scope() default FieldScope.USER;

    String[] alias() default {};

    UpdateParam update() default @UpdateParam;

    CreateParam create() default @CreateParam;

    SearchParam search() default @SearchParam;
}
