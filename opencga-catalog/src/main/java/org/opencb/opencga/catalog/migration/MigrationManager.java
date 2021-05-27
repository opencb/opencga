package org.opencb.opencga.catalog.migration;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.db.api.MigrationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MigrationManager {

    private CatalogManager catalogManager;
    private MigrationDBAdaptor migrationDBAdaptor;
    private final Logger logger;

    public MigrationManager(CatalogManager catalogManager, MigrationDBAdaptor migrationDBAdaptor) {
        this.catalogManager = catalogManager;
        this.migrationDBAdaptor = migrationDBAdaptor;
        this.logger = LoggerFactory.getLogger(MigrationManager.class);
    }

    public void runMigration(String version, Set<Migration.MigrationDomain> domainsFilter,
                             Set<Migration.MigrationLanguage> languageFilter, String token) throws CatalogException, MigrationException {
        runMigration(version, domainsFilter, languageFilter, new ObjectMap(), token);
    }

    public void runMigration(String version, Set<Migration.MigrationDomain> domainsFilter,
                             Set<Migration.MigrationLanguage> languageFilter, ObjectMap params, String token)
            throws MigrationException, CatalogException {
        validateAdmin(token);

        // 1. Fetch required migrations sorted by rank
        List<Class<? extends MigrationTool>> runnableMigrations = getRunnableMigrations(version, domainsFilter, languageFilter);

        // 2. Get pending migrations
        List<Class<? extends MigrationTool>> pendingMigrations = getPendingMigrations(version);

        // 2.1. Check that all pending migrations can be run automatically
        for (Class<? extends MigrationTool> migration : pendingMigrations) {
            Migration annotation = getMigrationAnnotation(migration);
            if (annotation.manual()) {
                throw new MigrationException("Missing previous migration '" + annotation.id() + "' from version '" + annotation.version()
                        + "' requiring manual interaction");
            }
        }

        // 3. Execute pending migrations
        for (Class<? extends MigrationTool> migration : pendingMigrations) {
            run(migration, new ObjectMap());
        }

        // 4. Execute target migration
        for (Class<? extends MigrationTool> migration : runnableMigrations) {
            run(migration, params);
        }
    }

    private List<Class<? extends MigrationTool>> getPendingMigrations(String version) throws MigrationException {
        // 2.1. Get all available migrations sorted to check if previous migrations have been run
        List<Class<? extends MigrationTool>> migrations = getAllSortedAvailableMigrations();

        // 2.2. Find position of first migration with version "version"
        int pos = 0;
        for (int i = 0; i < migrations.size(); i++) {
            Class<? extends MigrationTool> migration = migrations.get(i);
            Migration annotation = getMigrationAnnotation(migration);
            if (annotation == null) {
                throw new MigrationException("Class " + migration + " does not have the required java annotation @"
                        + Migration.class.getSimpleName());
            }
            if (annotation.version().equalsIgnoreCase(version)) {
                pos = i;
                break;
            }
        }
        if (pos == 0) {
            throw new MigrationException("Could not find migration for version '" + version + "'");
        }
        // Exclude newer migrations
        migrations = migrations.subList(0, pos);

        // 2.3. Remove migrations already run from list
        List<String> migrationIdList = migrations.stream().map(m -> getMigrationAnnotation(m).id()).collect(Collectors.toList());
        OpenCGAResult<MigrationRun> migrationResult = null;
        try {
            migrationResult = migrationDBAdaptor.get(migrationIdList);
        } catch (CatalogDBException e) {
            throw new MigrationException(e.getMessage(), e);
        }
        Map<String, MigrationRun> migrationResultMap = new HashMap<>(); // id: patch
        migrationResult.getResults().stream().map(m -> migrationResultMap.put(m.getId(), m));

        // Remove migrations if they have been run successfully with the proper patch
        migrations.removeIf(m -> {
            Migration annotation = getMigrationAnnotation(m);
            return annotation.patch() <= migrationResultMap.get(annotation.id()).getPatch()
                    && migrationResultMap.get(annotation.id()).getStatus() == MigrationRun.MigrationStatus.DONE;
        });
        return migrations;
    }

    private void validateAdmin(String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        catalogManager.getAuthorizationManager().checkIsInstallationAdministrator(userId);
    }

    private Set<Class<? extends MigrationTool>> getAvailableMigrations() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, Migration.class.getName()))
                )
                .filterInputsBy(input -> input != null && input.endsWith(".class"))
        );

        return reflections.getSubTypesOf(MigrationTool.class);
    }

    private List<Class<? extends MigrationTool>> getAllSortedAvailableMigrations() {
        List<Class<? extends MigrationTool>> migrations = new ArrayList<>(getAvailableMigrations());
        migrations.sort(this::compareTo);
        return migrations;
    }

    /**
     * Sort MigrationTools by version -> domain -> language -> rank.
     *
     * @param m1 MigrationTool 1.
     * @param m2 MigrationTool 2.
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
     */
    private int compareTo(Class<? extends MigrationTool> m1, Class<? extends MigrationTool> m2) {
        Migration m1Annotation = getMigrationAnnotation(m1);
        Migration m2Annotation = getMigrationAnnotation(m2);

        String[] m1VersionSplit = m1Annotation.version().split(".");
        String[] m2VersionSplit = m2Annotation.version().split(".");

        // 1. Check version
        // Check first version number
        if (Integer.parseInt(m1VersionSplit[0]) > Integer.parseInt(m2VersionSplit[0])) {
            return 1;
        } else if (Integer.parseInt(m1VersionSplit[0]) < Integer.parseInt(m2VersionSplit[0])) {
            return -1;
        }
        // Check second version number
        if (Integer.parseInt(m1VersionSplit[1]) > Integer.parseInt(m2VersionSplit[1])) {
            return 1;
        } else if (Integer.parseInt(m1VersionSplit[1]) < Integer.parseInt(m2VersionSplit[1])) {
            return -1;
        }
        m1VersionSplit = m1VersionSplit[2].split("-RC");
        m2VersionSplit = m2VersionSplit[2].split("-RC");
        // Check third version number
        if (Integer.parseInt(m1VersionSplit[0]) > Integer.parseInt(m2VersionSplit[0])) {
            return 1;
        } else if (Integer.parseInt(m1VersionSplit[0]) < Integer.parseInt(m2VersionSplit[0])) {
            return -1;
        }
        // Check for RC's
        if (m1VersionSplit.length == 2 && m2VersionSplit.length == 1) {
            return -1;
        } else if (m1VersionSplit.length == 1 && m2VersionSplit.length == 2) {
            return 1;
        } else if (m1VersionSplit.length == 2 && m2VersionSplit.length == 2) {
            if (Integer.parseInt(m1VersionSplit[1]) > Integer.parseInt(m2VersionSplit[1])) {
                return 1;
            } else if (Integer.parseInt(m1VersionSplit[1]) < Integer.parseInt(m2VersionSplit[1])) {
                return -1;
            }
        }

        // Domain
        int compare = m1Annotation.domain().compareTo(m2Annotation.domain());
        if (compare != 0) {
            return compare;
        }

        // Language
        compare = m1Annotation.language().compareTo(m2Annotation.language());
        if (compare != 0) {
            return compare;
        }

        // Rank
        if (m1Annotation.rank() > m2Annotation.rank()) {
            return 1;
        } else if (m1Annotation.rank() < m2Annotation.rank()) {
            return -1;
        }

        return 0;
    }


    private List<Class<? extends MigrationTool>> getRunnableMigrations(String version, Set<Migration.MigrationDomain> domainFilter,
                                                             Set<Migration.MigrationLanguage> languageFilter) {

        if (domainFilter == null || domainFilter.isEmpty()) {
            domainFilter = EnumSet.allOf(Migration.MigrationDomain.class);
        }
        if (languageFilter == null || languageFilter.isEmpty()) {
            languageFilter = EnumSet.allOf(Migration.MigrationLanguage.class);
        }

        Set<Class<? extends MigrationTool>> allMigrations = getAvailableMigrations();
        List<Class<? extends MigrationTool>> filteredMigrations = new LinkedList<>();

        for (Class<? extends MigrationTool> migration : allMigrations) {
            Migration annotation = getMigrationAnnotation(migration);

            if (StringUtils.isNotEmpty(version) && !annotation.version().equals(version)) {
                continue;
            }
            if (!domainFilter.isEmpty() && !domainFilter.contains(annotation.domain())) {
                continue;
            }
            if (!languageFilter.isEmpty() && !languageFilter.contains(annotation.language())) {
                continue;
            }
            filteredMigrations.add(migration);
        }

        return filteredMigrations;
    }

    private void run(Class<? extends MigrationTool> runnableMigration, ObjectMap params) throws MigrationException {
        Migration annotation = getMigrationAnnotation(runnableMigration);

        MigrationTool migrationTool;
        try {
            migrationTool = runnableMigration.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new MigrationException("Can't instantiate class " + runnableMigration + " from migration '" + annotation.id() + "'", e);
        }

        migrationTool.setup(catalogManager, params);

        logger.info("Executing migration '{}' for version '{}': {}", annotation.id(), annotation.version(), annotation.description());
        MigrationRun migrationRun = new MigrationRun(annotation.id(), annotation.description(), annotation.version(), TimeUtils.getDate(),
                annotation.patch());
        try {
            migrationTool.execute();
            migrationRun.setStatus(MigrationRun.MigrationStatus.DONE);
        } catch (MigrationException e) {
            migrationRun.setStatus(MigrationRun.MigrationStatus.ERROR);
            migrationRun.setException(e.getMessage());
            logger.error("Migration '{}' failed with message: {}", annotation.id(), e.getMessage(), e);
        } finally {
            migrationRun.setEnd(TimeUtils.getDate());
            try {
                migrationDBAdaptor.upsert(migrationRun);
            } catch (CatalogDBException e) {
                throw new MigrationException("Could not register migration in OpenCGA");
            }
        }
    }

    private Migration getMigrationAnnotation(Class<? extends MigrationTool> migration) {
        Migration annotation = migration.getAnnotation(Migration.class);
        return annotation;
    }

}
