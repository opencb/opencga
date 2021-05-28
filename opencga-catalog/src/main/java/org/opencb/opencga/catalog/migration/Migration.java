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
     * @return OpenCGA Version. Allowed formats: 1.2.3, 1.1.0-RC1
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
     * @return rank to know the execution order.
     */
    int rank();

    /**
     * @return migration patch.
     */
    int patch() default 1;

    /**
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
