package org.opencb.opencga.catalog.migration;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobInternal;
import org.opencb.opencga.core.models.job.JobReferenceParam;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.models.migration.MigrationRun;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.Status;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MigrationManager {

    private final CatalogManager catalogManager;
    private final Configuration configuration;

    private final Logger logger;
    private final MongoDBAdaptorFactory dbAdaptorFactory;

    public MigrationManager(CatalogManager catalogManager, DBAdaptorFactory dbAdaptorFactory, Configuration configuration) {
        this.catalogManager = catalogManager;
        this.configuration = configuration;
        this.dbAdaptorFactory = (MongoDBAdaptorFactory) dbAdaptorFactory;
        this.logger = LoggerFactory.getLogger(MigrationManager.class);
    }

    public List<MigrationRun> runManualMigration(String version, String id, Path appHome, ObjectMap params, String token)
            throws CatalogException {
        return runManualMigration(version, id, appHome, false, false, params, token);
    }

    public MigrationRun runManualMigration(String organizationId, String version, String id, Path appHome, boolean force, boolean offline,
                                           ObjectMap params, String token) throws CatalogException {
        token = validateAdmin(token);
        for (Class<? extends MigrationTool> c : getAvailableMigrations()) {
            Migration migration = getMigrationAnnotation(c);
            if (migration.id().equals(id) && migration.version().equals(version)) {
                MigrationRun migrationRun = updateMigrationRun(organizationId, migration, token);
                if (!offline && migration.offline()) {
                    throw MigrationException.offlineMigrationException(migration);
                }
                if (!force) {
                    switch (migrationRun.getStatus()) {
                        case DONE:
                            throw new MigrationException("Migration '" + id + "' already executed. Force migration run to continue.");
                        case ON_HOLD:
                            throw new MigrationException("Migration '" + id + "' holding jobs to finish. Force migration run to continue.");
                        case REDUNDANT:
                            throw new MigrationException("Migration '" + id + "' is not needed. Force migration run to continue.");
                        default:
                            break;
                    }
                }
                return run(organizationId, c, appHome, params, token);
            }
        }
        throw new MigrationException("Unable to find migration '" + id + "'");
    }

    public List<MigrationRun> runManualMigration(String version, String id, Path appHome, boolean force, boolean offline, ObjectMap params,
                                           String token) throws CatalogException {
        List<MigrationRun> migrationRunList = new LinkedList<>();
        // Migrate all organizations
        for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                migrationRunList.add(runManualMigration(organizationId, version, id, appHome, force, offline, params, token));
            }
        }
        // Lastly, migrate the admin organization
        migrationRunList.add(runManualMigration(ParamConstants.ADMIN_ORGANIZATION, version, id, appHome, force, offline, params, token));
        return migrationRunList;
    }

    public void runMigration(String version, Collection<Migration.MigrationDomain> domainsFilter,
                             Collection<Migration.MigrationLanguage> languageFilter, boolean offline, String appHome, String token)
            throws CatalogException, IOException {
        runMigration(version, domainsFilter, languageFilter, offline, appHome, new ObjectMap(), token);
    }

    public void runMigration(String version, Collection<Migration.MigrationDomain> domains,
                             Collection<Migration.MigrationLanguage> languages, boolean offline, String appHome, ObjectMap params,
                             String token) throws CatalogException, IOException {
        // Migrate all organizations
        for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                runMigration(organizationId, version, domains, languages, offline, appHome, params, token);
            }
        }
        // Lastly, migrate the admin organization
        runMigration(ParamConstants.ADMIN_ORGANIZATION, version, domains, languages, offline, appHome, params, token);
    }

    public void runMigration(String organizationId, String version, Collection<Migration.MigrationDomain> domains,
                Collection<Migration.MigrationLanguage> languages, boolean offline, String appHome, ObjectMap params,
                String token) throws CatalogException, IOException {

        logger.info("Running migrations for organization '{}'", organizationId);
        if (StringUtils.isNotEmpty(version)) {
            logger.info(" - Version : {}", version);
        }
        if (CollectionUtils.isNotEmpty(domains)) {
            logger.info(" - Domains : {}", domains);
        }
        if (CollectionUtils.isNotEmpty(languages)) {
            logger.info(" - Languages : {}", languages);
        }

        Path appHomePath = Paths.get(appHome);
        FileUtils.checkDirectory(appHomePath);

        token = validateAdmin(token);

        // 0. Fetch all migrations
        updateMigrationRuns(organizationId, token);
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();

        // 1. Fetch required migrations sorted by rank
        List<Class<? extends MigrationTool>> runnableMigrations = filterRunnableMigrations(organizationId, version, domains, languages,
                availableMigrations);

        // 2. Get pending migrations
        List<Class<? extends MigrationTool>> pendingMigrations = filterPendingMigrations(organizationId, version, availableMigrations);

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
            if (!offline && annotation.offline()) {
                throw MigrationException.offlineMigrationException(annotation);
            }
        }

        // 2.2. Check that all target migrations can be run automatically
        for (Class<? extends MigrationTool> migration : runnableMigrations) {
            Migration annotation = getMigrationAnnotation(migration);
            if (annotation.manual()) {
                throw new MigrationException("Migration '" + annotation.id() + "' from version '" + annotation.version()
                        + "' requires additional parameters and need to be run manually.");
            }
            if (!offline && annotation.offline()) {
                throw MigrationException.offlineMigrationException(annotation);
            }
        }

        // 3. Execute pending migrations
        for (Class<? extends MigrationTool> migration : pendingMigrations) {
            run(organizationId, migration, appHomePath, new ObjectMap(), token);
        }

        // 4. Execute target migration
        for (Class<? extends MigrationTool> migration : runnableMigrations) {
            run(organizationId, migration, appHomePath, params, token);
        }
    }

    public List<Class<? extends MigrationTool>> getPendingMigrations(String organizationId, String version, String token)
            throws CatalogException {
        token = validateAdmin(token);
        updateMigrationRuns(organizationId, token);
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();
        return filterPendingMigrations(organizationId, version, availableMigrations);
    }

    public Map<String, MigrationSummary> getMigrationSummary() throws CatalogException {
        Map<String, MigrationSummary> migrationSummaryMap = new HashMap<>();
        // Loop over organizations
        for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
            MigrationSummary migrationSummary = getMigrationSummary(organizationId);
            migrationSummaryMap.put(organizationId, migrationSummary);
        }
        return migrationSummaryMap;
    }

    private MigrationSummary getMigrationSummary(String organizationId) throws CatalogException {
        logger.info("Fetching migration summary for organization '{}'", organizationId);
        List<Pair<Migration, MigrationRun>> runs = getMigrationRuns(organizationId, null, null, null);

        MigrationSummary migrationSummary = new MigrationSummary()
                .setStatusCount(runs.stream().collect(Collectors.groupingBy(
                        p -> p.getValue().getStatus(),
                        () -> new EnumMap<>(MigrationRun.MigrationStatus.class),
                        Collectors.counting())))
                .setVersionCount(runs.stream().collect(Collectors.groupingBy(p -> p.getKey().version(), Collectors.counting())));

        long toBeApplied = migrationSummary
                .getStatusCount()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().toBeApplied())
                .mapToLong(Map.Entry::getValue)
                .sum();
        migrationSummary.setMigrationsToBeApplied(toBeApplied);

        return migrationSummary;
    }

    public List<Pair<Migration, MigrationRun>> getMigrationRuns(String organizationId, String token) throws CatalogException {
        return getMigrationRuns(organizationId, null, null, null, token);
    }

    public List<Pair<Migration, MigrationRun>> getMigrationRuns(String organizationId, String version,
                                                                List<Migration.MigrationDomain> domain, List<String> status, String token)
            throws CatalogException {
        token = validateAdmin(token);

        // 0. Update migration runs
        updateMigrationRuns(organizationId, token);

        return getMigrationRuns(organizationId, version, domain, status);
    }

    // This method should only be called when installing OpenCGA for the first time so it skips all available (and old) migrations.
    public void skipPendingMigrations(String organizationId, String token) throws CatalogException {
        validateAdmin(token);

        // 0. Fetch all migrations
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();

        // 1. Skip all available migrations
        for (Class<? extends MigrationTool> runnableMigration : availableMigrations) {
            Migration annotation = getMigrationAnnotation(runnableMigration);

            MigrationRun migrationRun = new MigrationRun(annotation.id(), annotation.description(), annotation.version(),
                    TimeUtils.getDate(), TimeUtils.getDate(), annotation.patch(), MigrationRun.MigrationStatus.REDUNDANT, "");
            try {
                dbAdaptorFactory.getMigrationDBAdaptor(organizationId).upsert(migrationRun);
            } catch (CatalogDBException e) {
                throw new MigrationException("Could not register migration in OpenCGA", e);
            }
        }
    }

    public void updateMigrationRuns(String token) throws CatalogException {
        // Loop over all organizations
        for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                updateMigrationRuns(organizationId, token);
            }
        }
        // Lastly, migrate the admin organization
        updateMigrationRuns(ParamConstants.ADMIN_ORGANIZATION, token);
    }

    public void updateMigrationRuns(String organizationId, String token) throws CatalogException {
        token = validateAdmin(token);

        // 0. Fetch all migrations
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();

        logger.info("Updating migration runs for organization '{}'", organizationId);
        // 1. Update migration run status
        for (Class<? extends MigrationTool> runnableMigration : availableMigrations) {
            updateMigrationRun(organizationId, getMigrationAnnotation(runnableMigration), token);
        }
    }

    private List<Migration> getMigrations() {
        Set<Class<? extends MigrationTool>> availableMigrations = getAvailableMigrations();
        List<Migration> migrations = new ArrayList<>(availableMigrations.size());
        for (Class<? extends MigrationTool> migrationClass : availableMigrations) {
            migrations.add(getMigrationAnnotation(migrationClass));
        }
        return migrations;
    }

    private List<Pair<Migration, MigrationRun>> getMigrationRuns(String organizationId, String version,
                                                                 List<Migration.MigrationDomain> domain, List<String> status)
            throws CatalogException {

        // 1. Get migrations and filter
        List<Migration> migrations = getMigrations();
        if (version != null) {
            migrations.removeIf(migration -> !migration.version().equals(version));
        }
        if (CollectionUtils.isNotEmpty(domain)) {
            migrations.removeIf(migration -> !domain.contains(migration.domain()));
        }

        // 2. Get migration runs and filter by status
        List<MigrationRun> migrationRuns = dbAdaptorFactory.getMigrationDBAdaptor(organizationId)
                .get(migrations.stream().map(Migration::id).collect(Collectors.toList()))
                .getResults();

        Map<String, Pair<Migration, MigrationRun>> map = new HashMap<>(migrations.size());
        // If no migrations can be found registered in the database is because they are actually not needed (new installation), otherwise
        // it will be because it hadn't been checked yet
        MigrationRun.MigrationStatus defaultMigrationStatus = migrationRuns.isEmpty()
                ? MigrationRun.MigrationStatus.REDUNDANT
                : MigrationRun.MigrationStatus.PENDING;
        for (Migration migration : migrations) {
            // Create dummy MigrationRun as if all migrations were already run
            MigrationRun migrationRun = new MigrationRun(migration.id(), migration.description(), migration.version(),
                    TimeUtils.getDate(), TimeUtils.getDate(), migration.patch(), defaultMigrationStatus, "");
            map.put(migration.id(), MutablePair.of(migration, migrationRun));
        }

        for (MigrationRun migrationRun : migrationRuns) {
            map.get(migrationRun.getId()).setValue(migrationRun);
        }
        if (CollectionUtils.isNotEmpty(status)) {
            map.values().removeIf(p -> !status.contains(p.getValue().getStatus().name()));
        }

        List<Pair<Migration, MigrationRun>> pairs = new ArrayList<>(map.values());
        pairs.sort(Comparator.<Pair<Migration, MigrationRun>, String>comparing(p -> p.getKey().version())
                .thenComparing(p -> p.getKey().date()));
        return pairs;
    }

    private MigrationRun updateMigrationRun(String organizationId, Migration migration, String token) throws CatalogException {
        MigrationRun migrationRun = dbAdaptorFactory.getMigrationDBAdaptor(organizationId).get(migration.id()).first();
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
                    if (migrationRun.getException() != null) {
                        migrationRun.setException(null);
                        updated = true;
                    }
                    break;
                case ON_HOLD:
                    // Check jobs
                    MigrationRun.MigrationStatus status = getOnHoldMigrationRunStatus(organizationId, migration, migrationRun, token);
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
            dbAdaptorFactory.getMigrationDBAdaptor(organizationId).upsert(migrationRun);
        }
        return migrationRun;
    }

    private MigrationRun.MigrationStatus getOnHoldMigrationRunStatus(String organizationId, Migration migration, MigrationRun migrationRun,
                                                                     String token) throws CatalogException {
        boolean allDone = true;
        boolean anyError = false;
        for (JobReferenceParam jobR : migrationRun.getJobs()) {
            Job job = catalogManager.getJobManager()
                    .get(jobR.getStudyId(), jobR.getId(), new QueryOptions(QueryOptions.INCLUDE, "id,internal"), token)
                    .first();
            String jobStatus = job.getInternal().getStatus().getId();
            if (jobStatus.equals(Enums.ExecutionStatus.ERROR)
                    || jobStatus.equals(Enums.ExecutionStatus.ABORTED)) {
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

    private List<Class<? extends MigrationTool>> filterPendingMigrations(String organizationId, String version,
                                                                         Set<Class<? extends MigrationTool>> availableMigrations)
            throws MigrationException {

        // 2.1. Sort all available migrations to check if previous migrations have been run
        List<Class<? extends MigrationTool>> migrations = new ArrayList<>(availableMigrations);
        migrations.sort(this::compareTo);

        // 2.2. Find position of first migration with version >= "version"
        int pos = -1;
        for (int i = 0; i < migrations.size(); i++) {
            Class<? extends MigrationTool> migration = migrations.get(i);
            Migration annotation = getMigrationAnnotation(migration);
            if (annotation == null) {
                throw new MigrationException("Class " + migration + " does not have the required java annotation @"
                        + Migration.class.getSimpleName());
            }

            if (compareVersion(annotation.version(), version) >= 0) {
                pos = i;
                break;
            }
        }
        if (pos == 0) {
            return Collections.emptyList();
        } else if (pos > 0) {
            // Exclude newer migrations
            migrations = migrations.subList(0, pos);
        }

        // Exclude successfully executed migrations
        filterOutExecutedMigrations(organizationId, migrations);
        return migrations;
    }

    private String validateAdmin(String token) throws CatalogException {
        JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
        catalogManager.getAuthorizationManager().checkIsOpencgaAdministrator(jwtPayload);
        // Extend token life
        return catalogManager.getUserManager().getNonExpiringToken(jwtPayload.getOrganization(), jwtPayload.getUserId(),
                Collections.emptyMap(), token);
    }

    private Set<Class<? extends MigrationTool>> getAvailableMigrations() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, Migration.class.getName()))
                )
                .addUrls(getUrls())
                .filterInputsBy(input -> input != null && input.endsWith(".class"))
        );

        Set<Class<? extends MigrationTool>> migrations = reflections.getSubTypesOf(MigrationTool.class);
        migrations.removeIf(c -> Modifier.isAbstract(c.getModifiers()));

        // Validate unique ids and rank
        Map<String, Set<String>> versionIdMap = new HashMap<>();

        for (Class<? extends MigrationTool> migration : migrations) {
            Migration annotation = getMigrationAnnotation(migration);

            if (!versionIdMap.containsKey(annotation.version())) {
                versionIdMap.put(annotation.version(), new HashSet<>());
            }
            if (versionIdMap.get(annotation.version()).contains(annotation.id())) {
                throw new IllegalStateException("Found duplicated migration id '" + annotation.id() + "' in version "
                        + annotation.version());
            }
            if (String.valueOf(annotation.date()).length() != 8) {
                throw new IllegalStateException("Found unexpected date '" + annotation.date() + "' in migration '" + annotation.id()
                        + "' from version " + annotation.version() + ". Date format is YYYYMMDD.");
            }
            versionIdMap.get(annotation.version()).add(annotation.id());
        }

        return migrations;
    }

    private static Collection<URL> getUrls() {
        // TODO: What if there are third party libraries that implement Tools?
        //  Currently they must contain "opencga" in the jar name.
        //  e.g.  acme-rockets-opencga-5.4.0.jar
        Collection<URL> urls = new LinkedList<>();
        for (URL url : ClasspathHelper.forPackage("org.opencb.opencga")) {
            String name = url.getPath().substring(url.getPath().lastIndexOf('/') + 1);
            if (name.isEmpty() || (name.contains("opencga") && !name.contains("opencga-hadoop-shaded"))) {
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

        int compareValue = compareVersion(m1Annotation.version(), m2Annotation.version());
        if (compareValue != 0) {
            return compareValue;
        }

        // Rank
        if (m1Annotation.date() > m2Annotation.date()) {
            return 1;
        } else if (m1Annotation.date() < m2Annotation.date()) {
            return -1;
        } else {
            // They are from the same date so it doesn't really matter which one goes first
            return 1;
        }
    }

    protected static boolean sameVersion(String version1, String version2) {
        return compareVersion(version1, version2) == 0;
    }

    protected static int compareVersion(String version1, String version2) {
        int[] m1VersionSplit = Arrays.stream(version1.split("\\.")).mapToInt(Integer::parseInt).toArray();
        int[] m2VersionSplit = Arrays.stream(version2.split("\\.")).mapToInt(Integer::parseInt).toArray();

        // 1. Check version
        // Check first version number
        if (m1VersionSplit[0] > m2VersionSplit[0]) {
            return 1;
        } else if (m1VersionSplit[0] < m2VersionSplit[0]) {
            return -1;
        }
        // Check second version number
        if (m1VersionSplit[1] > m2VersionSplit[1]) {
            return 1;
        } else if (m1VersionSplit[1] < m2VersionSplit[1]) {
            return -1;
        }

        // Check third version number
        if (m1VersionSplit[2] > m2VersionSplit[2]) {
            return 1;
        } else if (m1VersionSplit[2] < m2VersionSplit[2]) {
            return -1;
        }

        return 0;
    }

    private List<Class<? extends MigrationTool>> filterRunnableMigrations(String organizationId, String version,
                                                                          Collection<Migration.MigrationDomain> domain,
                                                                          Collection<Migration.MigrationLanguage> language,
                                                                          Set<Class<? extends MigrationTool>> allMigrations)
            throws MigrationException {

        if (CollectionUtils.isEmpty(domain)) {
            domain = EnumSet.allOf(Migration.MigrationDomain.class);
        }
        if (CollectionUtils.isEmpty(language)) {
            language = EnumSet.allOf(Migration.MigrationLanguage.class);
        }
        Predicate<Migration.MigrationDomain> domainFilter = domain::contains;
        Predicate<Migration.MigrationLanguage> languageFilter = language::contains;

        List<Class<? extends MigrationTool>> filteredMigrations = allMigrations.stream()
                .filter(m -> StringUtils.isEmpty(version) || sameVersion(getMigrationAnnotation(m).version(), version))
                .filter(m -> domainFilter.test(getMigrationAnnotation(m).domain()))
                .filter(m -> languageFilter.test(getMigrationAnnotation(m).language()))
                .sorted(this::compareTo)
                .collect(Collectors.toList());

        filterOutExecutedMigrations(organizationId, filteredMigrations);
        return filteredMigrations;
    }

    private MigrationRun run(String organizationId, Class<? extends MigrationTool> runnableMigration, Path appHome, ObjectMap params,
                             String token) throws MigrationException {
        Migration annotation = getMigrationAnnotation(runnableMigration);

        if (StringUtils.isNotEmpty(annotation.deprecatedSince())) {
            throw new MigrationException("Migration '" + annotation.id() + "' can't be run since version '" + annotation.deprecatedSince()
                    + "'. Please, run this migration from a previous OpenCGA version.");
        }

        MigrationTool migrationTool;
        try {
            migrationTool = runnableMigration.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new MigrationException("Can't instantiate class " + runnableMigration + " from migration '" + annotation.id() + "'", e);
        }

        Date start = TimeUtils.getDate();
        MigrationRun migrationRun;
        try {
            migrationRun = dbAdaptorFactory.getMigrationDBAdaptor(organizationId).get(annotation.id()).first();
            if (migrationRun == null) {
                migrationRun = new MigrationRun();
            }
            migrationRun.setStatus(MigrationRun.MigrationStatus.PENDING);
            migrationRun.setId(annotation.id());
            migrationRun.setDescription(annotation.description());
            migrationRun.setVersion(annotation.version());
            // Reset events
            migrationRun.setEvents(new LinkedList<>());
        } catch (CatalogDBException e) {
            throw new MigrationException("Error reading migration run from catalog", e);
        }
        migrationTool.setup(configuration, catalogManager, dbAdaptorFactory, migrationRun, organizationId, appHome, params, token);

        StopWatch stopWatch = StopWatch.createStarted();
        String path = Paths.get("JOBS")
                .resolve("opencga")
                .resolve(TimeUtils.getDay())
                .resolve(annotation.id()).toString();
        String jobId = "migration"
                + "-" + migrationRun.getId()
                + "-" + TimeUtils.getTime(start)
                + "-" + RandomStringUtils.randomAlphanumeric(5);
        String logFile = startMigrationLogger(jobId, Paths.get(configuration.getJobDir()).resolve(path));
        logger.info("------------------------------------------------------");
        logger.info("Executing migration '{}' for version '{}'", annotation.id(), annotation.version());
        logger.info("    {}", annotation.description());
        logger.info("------------------------------------------------------");

        MigrationException exceptionToThrow = null;
        try {
            MigrationRun.MigrationStatus status;
            if (annotation.domain() == Migration.MigrationDomain.STORAGE
                    && migrationTool.readStorageConfiguration().getMode() == StorageConfiguration.Mode.READ_ONLY) {
                status = MigrationRun.MigrationStatus.PENDING;
                String message = "Unable to run migration over STORAGE with mode " + StorageConfiguration.Mode.READ_ONLY;
                logger.info(message);
                migrationRun.addEvent(Event.Type.INFO,
                        message);
            } else {
                migrationTool.execute();
                if (migrationRun.getJobs().isEmpty()) {
                    status = MigrationRun.MigrationStatus.DONE;
                } else {
                    status = getOnHoldMigrationRunStatus(organizationId, migrationTool.getAnnotation(), migrationRun, token);
                }
            }
            // Clear exception
            migrationRun.setException(null);
            migrationRun.setStatus(status);
            logger.info("------------------------------------------------------");
            if (status == MigrationRun.MigrationStatus.DONE) {
                logger.info("Migration '{}' succeeded : {}", annotation.id(), TimeUtils.durationToString(stopWatch));
            } else if (status == MigrationRun.MigrationStatus.ON_HOLD) {
                logger.info("Migration '{}' on hold of pending jobs : {}", annotation.id(), TimeUtils.durationToString(stopWatch));
            } else if (status == MigrationRun.MigrationStatus.ERROR) {
                logger.info("Migration '{}' on ERROR as some jobs failed : {}", annotation.id(), TimeUtils.durationToString(stopWatch));
            } else if (status == MigrationRun.MigrationStatus.PENDING) {
                logger.info("Migration '{}' on PENDING as it could not be executed yet", annotation.id());
            } else {
                // Should not happen
                logger.info("Migration '{}' finished with status {} : {}", annotation.id(), status, TimeUtils.durationToString(stopWatch));
            }
            logger.info("------------------------------------------------------");
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
            logger.info("------------------------------------------------------");
        } finally {
            stopMigrationLogger();
            migrationRun.setStart(start);
            migrationRun.setEnd(TimeUtils.getDate());
            migrationRun.setPatch(annotation.patch());
            try {
                String adminStudy = ParamConstants.ADMIN_ORGANIZATION + "@admin:admin";
                dbAdaptorFactory.getMigrationDBAdaptor(organizationId).upsert(migrationRun);
                OpenCGAResult<File> outdir = catalogManager.getFileManager()
                        .createFolder(adminStudy, path, true, "Migration job " + migrationRun.getId(), null,
                                QueryOptions.empty(), token);
                OpenCGAResult<File> stderr = catalogManager.getFileManager()
                        .link(adminStudy, new FileLinkParams()
                                        .setPath(Paths.get(path, logFile).toString())
                                        .setUri(Paths.get(catalogManager.getConfiguration().getJobDir(), path, logFile).toUri().toString()),
                                false, token);

                Job job = new Job()
                        .setId(jobId)
                        .setDescription("Execution of migration '" + migrationRun.getId() + "'")
                        .setCreationDate(TimeUtils.getTime(start))
                        .setCommandLine("opencga-admin.sh")
                        .setParams(params)
                        .setTool(new ToolInfo(annotation.id(), annotation.description(), Tool.Scope.GLOBAL, null, null))
                        .setOutDir(outdir.first())
                        .setStderr(stderr.first())
                        .setInternal(new JobInternal()
                                .setEvents(migrationRun.getEvents()))
                        .setAttributes(new HashMap<>())
                        .setTags(Arrays.asList("migration", String.valueOf(annotation.domain()), annotation.version()));
                job.getAttributes().put("migrationRun", migrationRun);

                switch (migrationRun.getStatus()) {
                    case DONE:
                    case ON_HOLD:
                        job.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE));
                        job.setExecution(new ExecutionResult()
                                .setStart(migrationRun.getStart())
                                .setEnd(migrationRun.getEnd())
                                .setStatus(new Status(Status.Type.DONE, null, migrationRun.getEnd()))
                                .setEvents(migrationRun.getEvents()));
                        break;
                    case PENDING:
                        job.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED, "Could not be executed"));
                        break;
                    case ERROR:
                    default:
                        job.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, migrationRun.getException()));
                        job.setExecution(new ExecutionResult()
                                .setStart(migrationRun.getStart())
                                .setEnd(migrationRun.getEnd())
                                .setStatus(new Status(Status.Type.ERROR, null, migrationRun.getEnd()))
                                .setEvents(migrationRun.getEvents()));
                        break;
                }
                catalogManager.getJobManager().create(adminStudy, job, new QueryOptions(), token);
            } catch (CatalogException e) {
                exceptionToThrow = new MigrationException("Could not register migration in OpenCGA", e);
            }
        }
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return migrationRun;
    }

    private void filterOutExecutedMigrations(String organizationId, List<Class<? extends MigrationTool>> migrations)
            throws MigrationException {
        // Remove migrations successfully executed from list
        List<String> migrationIdList = migrations.stream().map(m -> getMigrationAnnotation(m).id()).collect(Collectors.toList());
        OpenCGAResult<MigrationRun> migrationResult;
        try {
            migrationResult = dbAdaptorFactory.getMigrationDBAdaptor(organizationId).get(migrationIdList);
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
        return migration.getAnnotation(Migration.class);
    }

    private String startMigrationLogger(String jobId, Path path) {
        // Create file appender
        String fileName = jobId + ".err";
        String fileNamePath = path.resolve(fileName).toAbsolutePath().toString();

        FileAppender fileAppender = FileAppender.newBuilder()
                .setName("MigrationRunAppender")
                .withAppend(true)
                .withCreateOnDemand(false)
                .setLayout(org.apache.logging.log4j.core.layout.PatternLayout.newBuilder()
                        .withPattern("%d{yyyy-MM-dd HH:mm:ss} [%t] %-5p %c{1}:%L - %m%n")
                        .build())
                .withFileName(fileNamePath)
                .build();
        fileAppender.start();
        addAppender(fileAppender);

        return fileName;
    }

    private void stopMigrationLogger() {
        //Restore logger configuration
        Configurator.shutdown(LoggerContext.getContext());
        Configurator.reconfigure();
    }

    void addAppender(final Appender appender) {
        final LoggerContext context = LoggerContext.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = context.getConfiguration();
        appender.start();
        config.addAppender(appender);

        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(appender, null, null);
        }
        config.getRootLogger().addAppender(appender, null, null);
    }
}
