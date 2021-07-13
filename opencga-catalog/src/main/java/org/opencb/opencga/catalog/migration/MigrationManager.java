package org.opencb.opencga.catalog.migration;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.MigrationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobReferenceParam;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class MigrationManager {

    private final CatalogManager catalogManager;
    private final Configuration configuration;
    private final MigrationDBAdaptor migrationDBAdaptor;

    private final Logger logger;
    private final MongoDBAdaptorFactory dbAdaptorFactory;

    public MigrationManager(CatalogManager catalogManager, DBAdaptorFactory dbAdaptorFactory, Configuration configuration) {
        this.catalogManager = catalogManager;
        this.configuration = configuration;
        this.migrationDBAdaptor = dbAdaptorFactory.getMigrationDBAdaptor();
        this.dbAdaptorFactory = (MongoDBAdaptorFactory) dbAdaptorFactory;
        this.logger = LoggerFactory.getLogger(MigrationManager.class);
    }

    public MigrationRun runManualMigration(String id, Path appHome, ObjectMap params, String token) throws CatalogException {
        validateAdmin(token);
        for (Class<? extends MigrationTool> c : getAvailableMigrations()) {
            Migration migrationAnnotation = getMigrationAnnotation(c);
            if (migrationAnnotation.id().equals(id)) {
                return run(c, appHome, params, token);
            }
        }
        throw new MigrationException("Unable to find migration '" + id + "'");
    }

    public void runMigration(String version, String appHome, String token) throws CatalogException, IOException {
        runMigration(version, Collections.emptySet(), Collections.emptySet(), appHome, new ObjectMap(), token);
    }

    public void runMigration(String version, Set<Migration.MigrationDomain> domainsFilter,
                             Set<Migration.MigrationLanguage> languageFilter, String appHome, String token)
            throws CatalogException, IOException {
        runMigration(version, domainsFilter, languageFilter, appHome, new ObjectMap(), token);
    }

    public void runMigration(String version, Set<Migration.MigrationDomain> domainsFilter,
                             Set<Migration.MigrationLanguage> languageFilter, String appHome, ObjectMap params, String token)
            throws CatalogException, IOException {
        Path appHomePath = Paths.get(appHome);
        FileUtils.checkDirectory(appHomePath);

        validateAdmin(token);

        // Extend token life
        token = catalogManager.getUserManager().getNonExpiringToken(AbstractManager.OPENCGA, token);

        // 0. Fetch all migrations
        updateMigrationRuns(token);
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();

        // 1. Fetch required migrations sorted by rank
        List<Class<? extends MigrationTool>> runnableMigrations = filterRunnableMigrations(version, domainsFilter, languageFilter,
                availableMigrations);

        // 2. Get pending migrations
        List<Class<? extends MigrationTool>> pendingMigrations = filterPendingMigrations(version, availableMigrations);

        if (runnableMigrations.isEmpty() && pendingMigrations.isEmpty()) {
            logger.info("Nothing to run. OpenCGA is up to date");
            return;
        }

        // 2.1. Check that all pending migrations can be run automatically
        for (Class<? extends MigrationTool> migration : pendingMigrations) {
            Migration annotation = getMigrationAnnotation(migration);
            if (annotation.manual()) {
                throw new MigrationException("Missing previous migration '" + annotation.id() + "' from version '" + annotation.version()
                        + "'. Please, run this migration manually using the CLI.");
            }
        }

        // 3. Execute pending migrations
        for (Class<? extends MigrationTool> migration : pendingMigrations) {
            run(migration, appHomePath, new ObjectMap(), token);
        }

        // 4. Execute target migration
        for (Class<? extends MigrationTool> migration : runnableMigrations) {
            run(migration, appHomePath, params, token);
        }
    }

    public List<Class<? extends MigrationTool>> getPendingMigrations(String version, String token) throws CatalogException {
        validateAdmin(token);
        updateMigrationRuns(token);
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();
        return filterPendingMigrations(version, availableMigrations);
    }

    public List<Migration> getMigrations(String token) throws CatalogException {
        validateAdmin(token);
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();
        List<Migration> migrations = new ArrayList<>(availableMigrations.size());
        for (Class<? extends MigrationTool> migrationClass : availableMigrations) {
            migrations.add(getMigrationAnnotation(migrationClass));
        }
        return migrations;
    }

    public List<Pair<Migration, MigrationRun>> getMigrationRuns(String token) throws CatalogException {
        return getMigrationRuns(null, null, null, token);
    }

    public List<Pair<Migration, MigrationRun>> getMigrationRuns(String version, List<Migration.MigrationDomain> domain,
                                                                List<String> status, String token) throws CatalogException {
        validateAdmin(token);

        // 0. Always update migration runs
        updateMigrationRuns(token);

        // 1. Get migrations and filter
        List<Migration> migrations = getMigrations(token);
        if (version != null) {
            migrations.removeIf(migration -> !migration.version().equals(version));
        }
        if (CollectionUtils.isNotEmpty(domain)) {
            migrations.removeIf(migration -> !domain.contains(migration.domain()));
        }
        Map<String, Pair<Migration, MigrationRun>> map = new HashMap<>(migrations.size());
        for (Migration migration : migrations) {
            map.put(migration.id(), MutablePair.of(migration, null));
        }

        // 2. Get migration runs and filter by status
        List<MigrationRun> migrationRuns = migrationDBAdaptor.get(migrations.stream().map(Migration::id).collect(Collectors.toList()))
                .getResults();
        for (MigrationRun migrationRun : migrationRuns) {
            map.get(migrationRun.getId()).setValue(migrationRun);
        }
        if (CollectionUtils.isNotEmpty(status)) {
            map.values().removeIf(p -> !status.contains(p.getValue().getStatus().name()));
        }

        List<Pair<Migration, MigrationRun>> pairs = new ArrayList<>(map.values());
        pairs.sort(Comparator.<Pair<Migration, MigrationRun>, String>comparing(p -> p.getKey().version())
                .thenComparing(p -> p.getKey().rank()));
        return pairs;
    }

    // This method should only be called when installing OpenCGA for the first time so it skips all available (and old) migrations.
    public void skipPendingMigrations(String token) throws CatalogException {
        validateAdmin(token);

        // 0. Fetch all migrations
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();

        // 1. Skip all available migrations
        for (Class<? extends MigrationTool> runnableMigration : availableMigrations) {
            Migration annotation = getMigrationAnnotation(runnableMigration);

            MigrationRun migrationRun = new MigrationRun(annotation.id(), annotation.description(), annotation.version(),
                    TimeUtils.getDate(), TimeUtils.getDate(), annotation.patch(), MigrationRun.MigrationStatus.REDUNDANT, "");
            try {
                migrationDBAdaptor.upsert(migrationRun);
            } catch (CatalogDBException e) {
                throw new MigrationException("Could not register migration in OpenCGA", e);
            }
        }
    }

    public void updateMigrationRuns(String token) throws CatalogException {
        validateAdmin(token);

        // 0. Fetch all migrations
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();

        // 1. Update migration run status
        for (Class<? extends MigrationTool> runnableMigration : availableMigrations) {
            updateMigrationRun(getMigrationAnnotation(runnableMigration), token);
        }

    }

    private void updateMigrationRun(Migration migration, String token) throws CatalogException {
        MigrationRun migrationRun = migrationDBAdaptor.get(migration.id()).first();
        boolean updated = false;

        if (migrationRun == null) {
            migrationRun = new MigrationRun();
            migrationRun.setStatus(MigrationRun.MigrationStatus.PENDING);
            migrationRun.setId(migration.id());
            migrationRun.setDescription(migration.description());
            migrationRun.setVersion(migration.version());
            updated = true;
        } else {
            switch (migrationRun.getStatus()) {
                case REDUNDANT:
                case DONE:
                    // Check patch
                    if (migrationRun.getPatch() != migration.patch()) {
                        migrationRun.setStatus(MigrationRun.MigrationStatus.OUTDATED);
                        updated = true;
                    }
                    break;
                case ON_HOLD:
                    // Check jobs
                    MigrationRun.MigrationStatus status = getOnHoldMigrationRunStatus(migration, migrationRun, token);
                    migrationRun.setStatus(status);
                    if (status != MigrationRun.MigrationStatus.ON_HOLD) {
                        updated = true;
                    }
                    break;
                case PENDING:
                case OUTDATED:
                case ERROR:
                    // Nothing to do
                    break;
                default:
                    throw new IllegalArgumentException("Unknown status " + migrationRun.getStatus());
            }
        }
        if (updated) {
            migrationDBAdaptor.upsert(migrationRun);
        }
    }

    private MigrationRun.MigrationStatus getOnHoldMigrationRunStatus(Migration migration, MigrationRun migrationRun, String token)
            throws CatalogException {
        boolean allDone = true;
        boolean anyError = false;
        for (JobReferenceParam jobR : migrationRun.getJobs()) {
            Job job = catalogManager.getJobManager()
                    .get(jobR.getStudyId(), jobR.getId(), new QueryOptions(QueryOptions.INCLUDE, "id,internal"), token).first();
            String jobStatus = job.getInternal().getStatus().getName();
            if (jobStatus.equals(Enums.ExecutionStatus.ERROR)) {
                anyError = true;
            }
            if (!jobStatus.equals(Enums.ExecutionStatus.DONE)) {
                allDone = false;
            }
        }

        if (anyError) {
            return MigrationRun.MigrationStatus.ERROR;
        } else if (allDone) {
            // It could get outdated
            if (migrationRun.getPatch() != migration.patch()) {
                return MigrationRun.MigrationStatus.OUTDATED;
            } else {
                return MigrationRun.MigrationStatus.DONE;
            }
        } else {
            return MigrationRun.MigrationStatus.ON_HOLD;
        }
    }

    private List<Class<? extends MigrationTool>> filterPendingMigrations(String version,
                                                                         Set<Class<? extends MigrationTool>> availableMigrations)
            throws MigrationException {

        // 2.1. Sort all available migrations to check if previous migrations have been run
        List<Class<? extends MigrationTool>> migrations = new ArrayList<>(availableMigrations);
        migrations.sort(this::compareTo);

        // 2.2. Find position of first migration with version "version"
        int pos = -1;
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
        if (pos == -1) {
            throw new MigrationException("Could not find migration for version '" + version + "'");
        } else if (pos == 0) {
            return Collections.emptyList();
        }
        // Exclude newer migrations
        migrations = migrations.subList(0, pos);

        // Exclude successfully executed migrations
        filterOutExecutedMigrations(migrations);
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
                .addUrls(getUrls())
                .filterInputsBy(input -> input != null && input.endsWith(".class") && !input.endsWith("StorageMigrationTool.class"))
        );

        Set<Class<? extends MigrationTool>> migrations = reflections.getSubTypesOf(MigrationTool.class);

        // Validate unique ids and rank
        Map<String, Set<String>> versionIdMap = new HashMap<>();
        Map<String, Set<Integer>> versionRankMap = new HashMap<>();

        for (Class<? extends MigrationTool> migration : migrations) {
            Migration annotation = getMigrationAnnotation(migration);

            if (!versionIdMap.containsKey(annotation.version())) {
                versionIdMap.put(annotation.version(), new HashSet<>());
                versionRankMap.put(annotation.version(), new HashSet<>());
            }
            if (versionIdMap.get(annotation.version()).contains(annotation.id())) {
                throw new IllegalStateException("Found duplicated migration id '" + annotation.id() + "' in version "
                        + annotation.version());
            }
            if (versionRankMap.get(annotation.version()).contains(annotation.rank())) {
                throw new IllegalStateException("Found duplicated migration rank " + annotation.rank() + " in version "
                        + annotation.version());
            }
            versionIdMap.get(annotation.version()).add(annotation.id());
            versionRankMap.get(annotation.version()).add(annotation.rank());
        }

        return migrations;
    }

    private static Collection<URL> getUrls() {
        // TODO: What if there are third party libraries that implement Tools?
        //  Currently they must contain "opencga" in the jar name.
        //  e.g.  acme-rockets-opencga-5.4.0.jar
        Collection<URL> urls = new LinkedList<>();
        for (URL url : ClasspathHelper.forClassLoader()) {
            String name = url.getPath().substring(url.getPath().lastIndexOf('/') + 1);
            if (name.isEmpty() || (name.contains("opencga") && !name.contains("opencga-storage-hadoop-deps"))) {
                urls.add(url);
            }
        }
        return urls;
    }

    /**
     * Sort MigrationTools by version -> rank.
     *
     * @param m1 MigrationTool 1.
     * @param m2 MigrationTool 2.
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
     */
    private int compareTo(Class<? extends MigrationTool> m1, Class<? extends MigrationTool> m2) {
        Migration m1Annotation = getMigrationAnnotation(m1);
        Migration m2Annotation = getMigrationAnnotation(m2);

        String[] m1VersionSplit = m1Annotation.version().split("\\.");
        String[] m2VersionSplit = m2Annotation.version().split("\\.");

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

        // Rank
        if (m1Annotation.rank() > m2Annotation.rank()) {
            return 1;
        } else if (m1Annotation.rank() < m2Annotation.rank()) {
            return -1;
        }

        throw new IllegalStateException("Found migration '" + m1Annotation.id() + "' and '" + m2Annotation.id() + "' with same rank "
                + m1Annotation.rank() + " for same version " + m1Annotation.version());
    }


    private List<Class<? extends MigrationTool>> filterRunnableMigrations(String version, Set<Migration.MigrationDomain> domainFilter,
                                                                          Set<Migration.MigrationLanguage> languageFilter,
                                                                          Set<Class<? extends MigrationTool>> allMigrations)
            throws MigrationException {

        if (domainFilter == null || domainFilter.isEmpty()) {
            domainFilter = EnumSet.allOf(Migration.MigrationDomain.class);
        }
        if (languageFilter == null || languageFilter.isEmpty()) {
            languageFilter = EnumSet.allOf(Migration.MigrationLanguage.class);
        }

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

        filteredMigrations.sort(this::compareTo);
        filterOutExecutedMigrations(filteredMigrations);
        return filteredMigrations;
    }

    private MigrationRun run(Class<? extends MigrationTool> runnableMigration, Path appHome, ObjectMap params, String token)
            throws MigrationException {
        Migration annotation = getMigrationAnnotation(runnableMigration);

        MigrationTool migrationTool;
        try {
            migrationTool = runnableMigration.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new MigrationException("Can't instantiate class " + runnableMigration + " from migration '" + annotation.id() + "'", e);
        }

        Date start = TimeUtils.getDate();
        MigrationRun migrationRun;
        try {
            migrationRun = migrationDBAdaptor.get(annotation.id()).first();
            if (migrationRun == null) {
                migrationRun = new MigrationRun();
            }
            migrationRun.setStatus(MigrationRun.MigrationStatus.PENDING);
            migrationRun.setId(annotation.id());
            migrationRun.setDescription(annotation.description());
            migrationRun.setVersion(annotation.version());
        } catch (CatalogDBException e) {
            throw new MigrationException("Error reading migration run from catalog", e);
        }
        migrationTool.setup(configuration, catalogManager, dbAdaptorFactory, migrationRun, appHome, params, token);

        StopWatch stopWatch = StopWatch.createStarted();
        logger.info("------------------------------------------------------");
        logger.info("Executing migration '{}' for version '{}'", annotation.id(), annotation.version());
        logger.info("    {}", annotation.description());
        logger.info("------------------------------------------------------");

        try {
            migrationTool.execute();
            logger.info("------------------------------------------------------");
            MigrationRun.MigrationStatus status;
            if (migrationRun.getJobs().isEmpty()) {
                status = MigrationRun.MigrationStatus.DONE;
            } else {
                status = getOnHoldMigrationRunStatus(migrationTool.getAnnotation(), migrationRun, token);
            }
            migrationRun.setStatus(status);
            if (status == MigrationRun.MigrationStatus.DONE) {
                logger.info("Migration '{}' succeeded : {}", annotation.id(), TimeUtils.durationToString(stopWatch));
            } else if (status == MigrationRun.MigrationStatus.ON_HOLD) {
                logger.info("Migration '{}' on hold of pending jobs : {}", annotation.id(), TimeUtils.durationToString(stopWatch));
            } else if (status == MigrationRun.MigrationStatus.ERROR) {
                logger.info("Migration '{}' on ERROR as some jobs failed : {}", annotation.id(), TimeUtils.durationToString(stopWatch));
            } else {
                // Should not happen
                logger.info("Migration '{}' finished with status {} : {}", annotation.id(), status, TimeUtils.durationToString(stopWatch));
            }
        } catch (Exception e) {
            migrationRun.setStatus(MigrationRun.MigrationStatus.ERROR);
            String message;
            if (e instanceof MigrationException && e.getCause() != null) {
                message = e.getCause().getMessage();
            } else {
                message = e.getMessage();
            }
            migrationRun.setException(message);
            logger.info("------------------------------------------------------");
            logger.error("Migration '{}' failed with message: {}", annotation.id(), message, e);
        } finally {
            migrationRun.setStart(start);
            migrationRun.setEnd(TimeUtils.getDate());
            migrationRun.setPatch(annotation.patch());
            try {
                migrationDBAdaptor.upsert(migrationRun);
            } catch (CatalogDBException e) {
                throw new MigrationException("Could not register migration in OpenCGA", e);
            }
        }
        return migrationRun;
    }

    private void filterOutExecutedMigrations(List<Class<? extends MigrationTool>> migrations) throws MigrationException {
        // Remove migrations successfully executed from list
        List<String> migrationIdList = migrations.stream().map(m -> getMigrationAnnotation(m).id()).collect(Collectors.toList());
        OpenCGAResult<MigrationRun> migrationResult;
        try {
            migrationResult = migrationDBAdaptor.get(migrationIdList);
        } catch (CatalogDBException e) {
            throw new MigrationException(e.getMessage(), e);
        }
        Map<String, MigrationRun> migrationResultMap = new HashMap<>(); // id: patch
        for (MigrationRun result : migrationResult.getResults()) {
            migrationResultMap.put(result.getId(), result);
        }

        // Remove migrations if:
        //  - They have been run successfully
        //  - They are redundant
        //  - They are on hold
        migrations.removeIf(m -> {
            Migration annotation = getMigrationAnnotation(m);
            if (!migrationResultMap.containsKey(annotation.id())) {
                return false;
            }
            return migrationResultMap.get(annotation.id()).getStatus() == MigrationRun.MigrationStatus.DONE
                    || migrationResultMap.get(annotation.id()).getStatus() == MigrationRun.MigrationStatus.REDUNDANT
                    || migrationResultMap.get(annotation.id()).getStatus() == MigrationRun.MigrationStatus.ON_HOLD;
        });
    }

    private Migration getMigrationAnnotation(Class<? extends MigrationTool> migration) {
        Migration annotation = migration.getAnnotation(Migration.class);
        return annotation;
    }

}
