package org.opencb.opencga.catalog.migration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Migration {

    /**
     * @return unique id.
     */
    String id();

    /**
     * @return migration description (Ticket that fixes).
     */
    String description();

    /**
     * @return OpenCGA Version. Must contain 3 numbers separated by dots, nothing else: 1.2.3 , 1.3.15 ...
     */
    String version();

    /**
     * @return affected OpenCGA domain.
     */
    MigrationDomain domain() default MigrationDomain.CATALOG;

    /**
     * @return language used to run the migration.
     */
    MigrationLanguage language() default MigrationLanguage.JAVA;

    /**
     * @return Migration script creation date [Format: YYYYMMDD].
     */
    int date();

    /**
     * @return migration patch.
     */
    int patch() default 1;

    /**
     * @return whether the migration requires that the database is offline or can be accessed by users while it runs.
     */
    boolean offline() default false;

    /**
     * @return whether the migration can be run as is or requires additional parameters that need to be passed by the administrator.
     */
    boolean manual() default false;

    /**
     * @return the version when the migration was deprecated.
     */
    String deprecatedSince() default "";

    enum MigrationDomain {
        CATALOG,
        STORAGE
    }

    enum MigrationLanguage {
        JAVA,
        JAVASCRIPT
    }
}
