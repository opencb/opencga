package org.opencb.opencga.app.cli.analysis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.analysis.AnalysisOutputRecorder;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 09/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AlignmentCommandExecutor extends CommandExecutor {
    private final AnalysisCliOptionsParser.AlignmentCommandOptions alignmentCommandOptions;
    private CatalogManager catalogManager;
    private StorageManagerFactory storageManagerFactory;
    private AlignmentStorageManager alignmentStorageManager;

    public AlignmentCommandExecutor(AnalysisCliOptionsParser.AlignmentCommandOptions options) {
        super(options.commonOptions);
        alignmentCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = alignmentCommandOptions.getParsedSubCommand();
        configure();
        switch (subCommandString) {
            case "index":
                index();
                break;
            case "query":
                query();
                break;
            case "stats":
                stats();
                break;
            case "delete":
                delete();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void configure()
            throws IllegalAccessException, ClassNotFoundException, InstantiationException, CatalogException {

        //  Creating CatalogManager
        catalogManager = new CatalogManager(catalogConfiguration);

        // Creating StorageManagerFactory
        storageManagerFactory = new StorageManagerFactory(storageConfiguration);

    }

    private AlignmentStorageManager initAlignmentStorageManager(DataStore dataStore)
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        String storageEngine = dataStore.getStorageEngine();
        if (StringUtils.isEmpty(storageEngine)) {
            this.alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager();
        } else {
            this.alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager(storageEngine);
        }
        return alignmentStorageManager;
    }

    private void index()
            throws CatalogException, AnalysisExecutionException, JsonProcessingException, IllegalAccessException, InstantiationException,
            ClassNotFoundException, StorageManagerException {
        AnalysisCliOptionsParser.IndexAlignmentCommandOptions cliOptions = alignmentCommandOptions.indexAlignmentCommandOptions;

        String sessionId = cliOptions.commonOptions.sessionId;
        long inputFileId = catalogManager.getFileId(cliOptions.fileId);

        // 1) Create, if not provided, an indexation job
        if (StringUtils.isEmpty(cliOptions.job.jobId)) {
            long outDirId;
            if (cliOptions.outdirId == null) {
                outDirId = catalogManager.getFileParent(inputFileId, null, sessionId).first().getId();
            } else  {
                outDirId = catalogManager.getFileId(cliOptions.outdirId);
            }

            AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);

            List<String> extraParams = cliOptions.commonOptions.params.entrySet()
                    .stream()
                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.toList());

            QueryOptions options = new QueryOptions()
                    .append(AnalysisJobExecutor.EXECUTE, !cliOptions.job.queue)
                    .append(AnalysisJobExecutor.SIMULATE, false)
                    .append(AnalysisFileIndexer.TRANSFORM, cliOptions.transform)
                    .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                    .append(AnalysisFileIndexer.PARAMETERS, extraParams)
                    .append(AnalysisFileIndexer.LOG_LEVEL, cliOptions.commonOptions.logLevel);

            QueryResult<Job> result = analysisFileIndexer.index(inputFileId, outDirId, sessionId, options);
            if (cliOptions.job.queue) {
                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(result));
            }

        } else {
            long studyId = catalogManager.getStudyIdByFileId(inputFileId);
            index(getJob(studyId, cliOptions.job.jobId, sessionId));
        }
    }

    private void index(Job job) throws CatalogException, IllegalAccessException, ClassNotFoundException, InstantiationException, StorageManagerException {

        AnalysisCliOptionsParser.IndexAlignmentCommandOptions cliOptions = alignmentCommandOptions.indexAlignmentCommandOptions;


        String sessionId = cliOptions.commonOptions.sessionId;
        long inputFileId = catalogManager.getFileId(cliOptions.fileId);

        // 1) Initialize VariantStorageManager
        long studyId = catalogManager.getStudyIdByFileId(inputFileId);
        Study study = catalogManager.getStudy(studyId, sessionId).first();

        /*
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used from Catalog
         */
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.ALIGNMENT, sessionId);
        initAlignmentStorageManager(dataStore);


        // 2) Read and validate cli args. Configure options
        ObjectMap alignmentOptions = alignmentStorageManager.getOptions();
        if (Integer.parseInt(cliOptions.fileId) != 0) {
            alignmentOptions.put(AlignmentStorageManager.Options.FILE_ID.key(), cliOptions.fileId);
        }

        alignmentOptions.put(AlignmentStorageManager.Options.DB_NAME.key(), dataStore.getDbName());

        if (cliOptions.commonOptions.params != null) {
            alignmentOptions.putAll(cliOptions.commonOptions.params);
        }

        alignmentOptions.put(AlignmentStorageManager.Options.PLAIN.key(), false);
        alignmentOptions.put(AlignmentStorageManager.Options.INCLUDE_COVERAGE.key(), cliOptions.calculateCoverage);
        if (cliOptions.meanCoverage != null && !cliOptions.meanCoverage.isEmpty()) {
            alignmentOptions.put(AlignmentStorageManager.Options.MEAN_COVERAGE_SIZE_LIST.key(), cliOptions.meanCoverage);
        }
        alignmentOptions.put(AlignmentStorageManager.Options.COPY_FILE.key(), false);
        alignmentOptions.put(AlignmentStorageManager.Options.ENCRYPT.key(), "null");
        logger.debug("Configuration options: {}", alignmentOptions.toJson());


        final boolean doExtract;
        final boolean doTransform;
        final boolean doLoad;
        StorageETLResult storageETLResult = null;
        Exception exception = null;

        File file = catalogManager.getFile(inputFileId, sessionId).first();
        URI inputUri = catalogManager.getFileUri(file);
//        FileUtils.checkFile(Paths.get(inputUri.getPath()));

        URI outdirUri = job.getTmpOutDirUri();
//        FileUtils.checkDirectory(Paths.get(outdirUri.getPath()));


        if (!cliOptions.load && !cliOptions.transform) {  // if not present --transform nor --load,
            // do both
            doExtract = true;
            doTransform = true;
            doLoad = true;
        } else {
            doExtract = cliOptions.transform;
            doTransform = cliOptions.transform;
            doLoad = cliOptions.load;
        }

        // 3) Execute indexation
        try {
            storageETLResult = alignmentStorageManager.index(Collections.singletonList(inputUri), outdirUri, doExtract, doTransform, doLoad).get(0);

        } catch (StorageETLException e) {
            storageETLResult = e.getResults().get(0);
            exception = e;
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            exception = e;
            e.printStackTrace();
            throw e;
        } finally {
            // 4) Save indexation result.
            new AnalysisOutputRecorder(catalogManager, sessionId).saveStorageResult(job, storageETLResult);
        }
    }

    private void query() {
        throw new UnsupportedOperationException();
    }

    private void stats() {
        throw new UnsupportedOperationException();
    }

    private void delete() {
        throw new UnsupportedOperationException();
    }

    protected Job getJob(long studyId, String jobId, String sessionId) throws CatalogException {
        return catalogManager.getAllJobs(studyId, new Query(CatalogJobDBAdaptor.QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key() + "." + Job.JOB_SCHEDULER_NAME, jobId), null, sessionId).first();
    }

}
