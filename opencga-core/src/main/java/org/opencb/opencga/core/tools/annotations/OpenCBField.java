package org.opencb.opencga.core.tools.annotations;

public @interface OpenCBField {

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

    UpdateParam update() default @UpdateParam;

    CreateParam create() default @CreateParam;

    SearchParam search() default @SearchParam;
}
