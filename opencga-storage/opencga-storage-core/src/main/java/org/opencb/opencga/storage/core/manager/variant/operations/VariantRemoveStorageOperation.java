package org.opencb.opencga.storage.core.manager.variant.operations;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.FileIndex;
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
        DataStore dataStore = studyInfo.getDataStores().get(File.Bioformat.VARIANT);

        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        updateCatalogFromStudyConfiguration(sessionId, studyInfo.getStudyFQN(), dataStore);

        List<String> fileNames = new ArrayList<>(studyInfo.getFileInfos().size());
        List<String> filePaths = new ArrayList<>(studyInfo.getFileInfos().size());
        for (FileInfo fileInfo : studyInfo.getFileInfos()) {
            File file = catalogManager.getFileManager().get(studyInfo.getStudyFQN(), fileInfo.getPath(), null, sessionId).first();
            if (file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                fileNames.add(fileInfo.getName());
                filePaths.add(fileInfo.getPath());
            } else {
                throw new CatalogException("Unable to remove variants from file " + file.getName() + ". "
                        + "IndexStatus = " + file.getIndex().getStatus().getName());
            }
        }

        if (fileNames.isEmpty()) {
            throw new CatalogException("Nothing to do!");
        }

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
        variantStorageEngine.getOptions().putAll(options);

        variantStorageEngine.removeFiles(studyInfo.getStudyFQN(), fileNames);


        // Update study configuration to synchronize
        updateCatalogFromStudyConfiguration(sessionId, studyInfo.getStudyFQN(), dataStore);

        return catalogManager.getFileManager().get(studyInfo.getStudyFQN(),
                new Query(FileDBAdaptor.QueryParams.PATH.key(), filePaths), new QueryOptions(), sessionId)
                .getResult();
    }

    public void removeStudy(StudyInfo studyInfo, QueryOptions options, String sessionId)
            throws CatalogException, StorageEngineException, IOException {

        // We get the credentials of the Datastore to insert the variants
        DataStore dataStore = studyInfo.getDataStores().get(File.Bioformat.VARIANT);

        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        updateCatalogFromStudyConfiguration(sessionId, studyInfo.getStudyFQN(), dataStore);

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
        variantStorageEngine.getOptions().putAll(options);

        variantStorageEngine.removeStudy(studyInfo.getStudyFQN());


        // Update study configuration to synchronize
        updateCatalogFromStudyConfiguration(sessionId, studyInfo.getStudyFQN(), dataStore);

    }

}
