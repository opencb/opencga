package org.opencb.opencga.storage.core.local.variant.operations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.local.models.StudyInfo;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExportStorageOperation extends StorageOperation {

    public VariantExportStorageOperation(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, StorageManagerFactory.get(storageConfiguration),
                LoggerFactory.getLogger(VariantExportStorageOperation.class));
    }

    public List<File> exportData(List<StudyInfo> studyInfos, Query query, String outputFormat, String outdirStr,
                                 String sessionId, ObjectMap options)
            throws IOException, StorageManagerException, URISyntaxException, CatalogException {
        if (options == null) {
            options = new ObjectMap();
        }

        // Outdir must be empty
        List<File> newFiles;


        if (studyInfos.isEmpty()) {
            logger.warn("Nothing to do!");
            return Collections.emptyList();
        }

        Thread hook = null;
        URI outputFile = null;
        final Path outdir;
        if (StringUtils.isNoneEmpty(outdirStr)) {
            URI outdirUri = UriUtils.createDirectoryUri(outdirStr);
            outdir = Paths.get(outdirUri);
            outdirMustBeEmpty(outdir);

            hook = buildHook(outdir);
            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job has just started"));
            Runtime.getRuntime().addShutdownHook(hook);


            List<Region> regions = Region.parseRegions(query.getString(VariantDBAdaptor.VariantQueryParams.REGION.key()));
            String outputFileName = buildOutputFileName(studyInfos.stream().map(StudyInfo::getStudyAlias).collect(Collectors.toList()),
                    regions, outputFormat);
            outputFile = outdirUri.resolve(outputFileName);
        } else {
            outdir = null;
        }



        // Up to this point, catalog has not been modified
        try {
            DataStore dataStore = studyInfos.get(0).getDataStores().get(File.Bioformat.VARIANT);
            for (StudyInfo studyInfo : studyInfos) {
                if (!studyInfo.getDataStores().get(File.Bioformat.VARIANT).equals(dataStore)) {
                    throw new StorageManagerException("Unable to export variants from studies in different databases");
                }
            }

//            String outputFileName = buildOutputFileName(Collections.singletonList(study.getAlias()), regions, outputFormat);
            Long catalogOutDirId = getCatalogOutdirId(studyInfos.get(0).getStudyId(), options, sessionId);

            // TODO: Needed?
            for (StudyInfo studyInfo : studyInfos) {
                StudyConfiguration studyConfiguration = updateStudyConfiguration(sessionId, studyInfo.getStudyId(), dataStore);
            }

            VariantStorageManager variantStorageManager = storageManagerFactory.getVariantStorageManager(dataStore.getStorageEngine());
            variantStorageManager.exportData(outputFile, outputFormat, dataStore.getDbName(), query, new QueryOptions(options));

            if (catalogOutDirId != null && outdir != null) {
                newFiles = copyResults(outdir, catalogOutDirId, sessionId);
            } else {
                newFiles = Collections.emptyList();
            }

            if (outdir != null) {
                writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.DONE, "Job completed"));
            }
        } catch (Exception e) {
            // Error!
            logger.error("Error exporting variants.", e);
            if (outdir != null) {
                writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job with error : " + e.getMessage()));
            }
            throw new StorageManagerException("Error exporting variants.", e);
        } finally {
            // Remove hook
            if (hook != null) {
                Runtime.getRuntime().removeShutdownHook(hook);
            }
        }

        return newFiles;
    }

    private String buildOutputFileName(List<String> studyNames, List<Region> regions, String format) {
        String studies = String.join("_", studyNames);
        if (regions == null || regions.size() != 1) {
            return studies + ".export";
        } else {
            return studies + '.' + regions.get(0).toString() + ".export";
        }
    }
}
