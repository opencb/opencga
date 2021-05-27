package org.opencb.opencga.catalog.migration;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.db.api.MigrationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.util.*;

public class MigrationManager {

    private CatalogManager catalogManager;
    private MigrationDBAdaptor migrationDBAdaptor;

    public MigrationManager(CatalogManager catalogManager, MigrationDBAdaptor migrationDBAdaptor) {
        this.catalogManager = catalogManager;
        this.migrationDBAdaptor = migrationDBAdaptor;
    }

    public void getMigrationsStatus(String token) throws CatalogException {
        validateAdmin(token);
    }

    public void updateMigrationsStatus(String token) throws CatalogException {
        validateAdmin(token);
    }

    public void runMigration(String version, Set<Migration.MigrationDomain> domainsFilter,
                             Set<Migration.MigrationLanguage> languageFilter, String token) throws CatalogException {

    }

    public void runMigration(String version, Set<Migration.MigrationDomain> domainsFilter,
                             Set<Migration.MigrationLanguage> languageFilter, ObjectMap params, String token) throws CatalogException {
        validateAdmin(token);

        if (domainsFilter == null || domainsFilter.isEmpty()) {
            domainsFilter = EnumSet.allOf(Migration.MigrationDomain.class);
        }
        if (languageFilter == null || languageFilter.isEmpty()) {
            languageFilter = EnumSet.allOf(Migration.MigrationLanguage.class);
        }

        // Get runnable migrations
        List<Class<? extends MigrationTool>> runnableMigrations = getRunnableMigrations(version, domainsFilter, languageFilter);

        while (!runnableMigrations.isEmpty()) {
            for (Class<? extends MigrationTool> runnableMigration : runnableMigrations) {
                run(runnableMigration, params);
            }

            // Refresh list of runnable migrations
            runnableMigrations = getRunnableMigrations(version, domainsFilter, languageFilter);
        }

        // Check that there are no pending migrations
        for (Class<MigrationTool> migration : migrations) {
            // check domain
            if (!domainsFilter.contains(getMigrationAnnotation(migration).domain())) {
                continue;
            }

            // Check status and patch
            MigrationRun migrationRun = getMigrationRun(migration);
            if (migrationRun == null) {
                // Cry
            }
            if (!migrationRun.getStatus().equals(MigrationRun.MigrationStatus.DONE)) {
                // Cry
            }
            if (getMigrationAnnotation(migration).patch() != migrationRun.getPatch()) {
                // Cry
            }
        }
    }

    private void validateAdmin(String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        if (!catalogManager.getAuthorizationManager().checkIsAdmin(userId)) {
            throw CatalogAuthorizationException.adminOnlySupportedOperation();
        }
    }

    private Set<Class<? extends MigrationTool>> getMigrations() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, Migration.class.getName()))
                )
                .filterInputsBy(input -> input != null && input.endsWith(".class"))
        );

        return reflections.getSubTypesOf(MigrationTool.class);
    }

    private List<Class<? extends MigrationTool>> getRunnableMigrations(String version, Set<Migration.MigrationDomain> domainFilter,
                                                             Set<Migration.MigrationLanguage> languageFilter) throws CatalogDBException {
        Set<Class<? extends MigrationTool>> allMigrations = getMigrations();
        Map<String, Class<? extends MigrationTool>> migrationMap = new HashMap<>();
        List<Class<? extends MigrationTool>> filteredMigrations = new LinkedList<>();

        for (Class<? extends MigrationTool> migration : allMigrations) {
            Migration annotation = migration.getAnnotation(Migration.class);
            migrationMap.put(annotation.id(), migration);

            if (StringUtils.isNotEmpty(version) && !annotation.version().equals(version)) {
                continue;
            }
            if (domainFilter != null && !domainFilter.isEmpty() && !domainFilter.contains(annotation.domain())) {
                continue;
            }
            if (languageFilter != null && !languageFilter.isEmpty() && !languageFilter.contains(annotation.language())) {
                continue;
            }
            filteredMigrations.add(migration);
        }

        // Check if it has been executed
        Iterator<Class<? extends MigrationTool>> iterator = filteredMigrations.iterator();
        while (iterator.hasNext()) {
            Class<? extends MigrationTool> filteredMigration = iterator.next();
            Migration annotation = filteredMigration.getAnnotation(Migration.class);
            OpenCGAResult<MigrationRun> migrationResult = migrationDBAdaptor.get(annotation.id());
            if (migrationResult.getNumResults() == 1) {
                if (annotation.patch() == migrationResult.first().getPatch()
                        && migrationResult.first().getStatus() == MigrationRun.MigrationStatus.DONE) {
                    iterator.remove();
                    continue;
                }
            }

            // Check dependencies
            if (annotation.requires().length > 0) {
                migrationResult = migrationDBAdaptor.get(Arrays.asList(annotation.requires()));
                Map<String, >
            }


        }
        // TODO
        // Get those that:
        //  Match type filter
        //  Has not been executed yet
        //  Have dependencies successfully executed
        return Collections.emptyList();
    }

    private void run(Class<? extends MigrationTool> runnableMigration) throws MigrationException {
        Migration annotation = runnableMigration.getAnnotation(Migration.class);
        if (annotation == null) {
            throw new MigrationException("Class " + runnableMigration + " does not have the required java annotation @"
                    + Migration.class.getSimpleName());
        }

        MigrationTool migrationTool;
        try {
            migrationTool = runnableMigration.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new MigrationException("Can't instantiate class " + runnableMigration + " from migration '" + annotation.id() + "'", e);
        }

        migrationTool.setup(catalogManager);
        migrationTool.
    }

    private MigrationRun getMigrationRun(Class<MigrationTool> migration) {
        return null;
    }

    private Migration getMigrationAnnotation(Class<MigrationTool> migration) {
        return migration.getAnnotation(Migration.class);
    }

}
