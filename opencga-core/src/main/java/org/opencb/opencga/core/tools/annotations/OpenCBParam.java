package org.opencb.opencga.core.tools.annotations;

public @interface OpenCBParam {

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

    UpdateParam update();

    CreateParam create();

    SearchParam search();
}
