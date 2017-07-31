package org.opencb.opencga.storage.core.manager.variant.operations;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileIndex;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.models.FileInfo;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 07/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantRemoveStorageOperation extends StorageOperation {

    public VariantRemoveStorageOperation(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory, LoggerFactory.getLogger(VariantRemoveStorageOperation.class));
    }

    public List<File> removeFiles(StudyInfo studyInfo, QueryOptions options, String sessionId)
            throws CatalogException, StorageEngineException, IOException {

        // We get the credentials of the Datastore to insert the variants
        DataStore dataStore = studyInfo.getDataStores().get(org.opencb.opencga.catalog.models.File.Bioformat.VARIANT);

        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        updateStudyConfiguration(sessionId, studyInfo.getStudyId(), dataStore);

        List<String> files = new ArrayList<>(studyInfo.getFileInfos().size());
        for (FileInfo fileInfo : studyInfo.getFileInfos()) {
            org.opencb.opencga.catalog.models.File file = catalogManager.getFile(fileInfo.getFileId(), sessionId).first();
            if (file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                files.add(String.valueOf(fileInfo.getFileId()));
            } else {
                throw new CatalogException("Unable to remove variants from file " + file.getName() + ". "
                        + "IndexStatus = " + file.getIndex().getStatus().getName());
            }
        }

        if (files.isEmpty()) {
            throw new CatalogException("Nothing to do!");
        }

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
        variantStorageEngine.getOptions().putAll(options);

        variantStorageEngine.removeFiles(String.valueOf(studyInfo.getStudyId()), files);


        // Update study configuration to synchronize
        updateStudyConfiguration(sessionId, studyInfo.getStudyId(), dataStore);

        return catalogManager.getAllFiles(studyInfo.getStudyId(),
                new Query(FileDBAdaptor.QueryParams.ID.key(), files),
                new QueryOptions(), sessionId)
                .getResult();
    }

    public void removeStudy(StudyInfo studyInfo, QueryOptions options, String sessionId)
            throws CatalogException, StorageEngineException, IOException {

        // We get the credentials of the Datastore to insert the variants
        DataStore dataStore = studyInfo.getDataStores().get(org.opencb.opencga.catalog.models.File.Bioformat.VARIANT);

        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        updateStudyConfiguration(sessionId, studyInfo.getStudyId(), dataStore);

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
        variantStorageEngine.getOptions().putAll(options);

        variantStorageEngine.removeStudy(String.valueOf(studyInfo.getStudyId()));


        // Update study configuration to synchronize
        updateStudyConfiguration(sessionId, studyInfo.getStudyId(), dataStore);

    }

}
