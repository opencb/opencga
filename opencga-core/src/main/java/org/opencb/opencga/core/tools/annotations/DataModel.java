package org.opencb.opencga.core.tools.annotations;

public @interface DataModel {

    /**
     * @return id of the field.
     */
    String id() default "";

    /**
     * @return name of the field.
     */
    String name() default "";

    /**
     * @return name of the field.
     */
    String description() default "";

    /**
     * @return Class of field.
     */
    Class<?> dataTypeClass() default Void.class;

    boolean indexed() default false;

    boolean required() default false;

    boolean managed() default false;

    boolean unique() default false;

    boolean deprecated() default false;

    int since() default -1;

    String[] dependsOn() default {};

    FieldScope scope() default FieldScope.USER;

    String[] alias() default {};

    UpdateParam update() default @UpdateParam;

    CreateParam create() default @CreateParam;

    SearchParam search() default @SearchParam;
}
