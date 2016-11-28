package org.opencb.opencga.storage.core.local.variant.operations;

import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created on 24/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationStorageOperation extends StorageOperation {

    public VariantAnnotationStorageOperation(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, StorageManagerFactory.get(storageConfiguration),
                LoggerFactory.getLogger(VariantAnnotationStorageOperation.class));
    }

    public void annotateVariants(long studyId, Query query, String outdirStr, String catalogOutDir, String sessionId, ObjectMap options)
            throws CatalogException, StorageManagerException, URISyntaxException, IOException {
        if (options == null) {
            options = new ObjectMap();
        }

        // Outdir must be empty
        URI outdirUri = UriUtils.createDirectoryUri(outdirStr);
        final Path outdir = Paths.get(outdirUri);
        outdirMustBeEmpty(outdir);

        Thread hook = buildHook(outdir);
        writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job has just started"));
        Runtime.getRuntime().addShutdownHook(hook);
        // Up to this point, catalog has not been modified
        try {

            Study study = catalogManager.getStudy(studyId, sessionId).first();
            List<Region> regions = Region.parseRegions(query.getString(VariantQueryParams.REGION.key()));
            String outputFileName = buildOutputFileName(study.getAlias(), regions);

            Long catalogOutDirId = getCatalogOutdirId(studyId, catalogOutDir, sessionId);

            Query annotationQuery = new Query(query);
            if (!options.getBoolean(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, false)) {
                annotationQuery.put(VariantQueryParams.ANNOTATION_EXISTS.key(), false);
            }
//            annotationQuery.put(VariantQueryParams.STUDIES.key(),
//                    Collections.singletonList(studyId));

            QueryOptions annotationOptions = new QueryOptions(options)
                    .append(DefaultVariantAnnotationManager.OUT_DIR, outdirUri.getPath());
            options.putIfAbsent(DefaultVariantAnnotationManager.FILE_NAME, outputFileName);

            DataStore dataStore = StorageOperation.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
            // TODO: Needed?
            StudyConfiguration studyConfiguration = updateStudyConfiguration(sessionId, studyId, dataStore);

            VariantStorageManager variantStorageManager = storageManagerFactory.getVariantStorageManager(dataStore.getStorageEngine());
            variantStorageManager.annotate(dataStore.getDbName(), annotationQuery, annotationOptions);

            if (catalogOutDirId != null) {
                copyResults(Paths.get(outdirUri), catalogOutDirId, sessionId);
            }

            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.DONE, "Job completed"));
        } catch (Exception e) {
            // Error!
            logger.error("Error annotating variants.", e);
            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job with error : " + e.getMessage()));
            throw new StorageManagerException("Error annotating variants.", e);
        } finally {
            // Remove hook
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    private String buildOutputFileName(String studyName, List<Region> regions) {
        if (regions == null || regions.size() != 1) {
            return studyName + "." + TimeUtils.getTime();
        } else {
            return studyName + "." + regions.get(0).toString() + "." + TimeUtils.getTime();
        }
    }
}
