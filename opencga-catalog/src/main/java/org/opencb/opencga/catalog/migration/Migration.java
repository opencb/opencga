package org.opencb.opencga.catalog.migration;

public @interface Migration {

    String id();

    String description() default "";

    /**
     * @return OpenCGA Version
     */
    String version();

    MigrationDomain domain() default MigrationDomain.CATALOG;

    MigrationLanguage language() default MigrationLanguage.JAVA;

    String[] requires() default {};

    int patch() default 1;

    /**
     *
     * @return whether the migration can be run as is or requires additional parameters that need to be passed by the administrator.
     */
    boolean manual() default false;

    enum MigrationDomain {
        CATALOG,
        STORAGE
    }

    enum MigrationLanguage {
        JAVA,
        JAVASCRIPT
    }
}
