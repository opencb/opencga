package org.opencb.opencga.analysis.variant.operations;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.FileIndex;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
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

    public List<File> removeFiles(String study, List<String> files, QueryOptions options, String sessionId)
            throws CatalogException, StorageEngineException, IOException {

        // We get the credentials of the Datastore to insert the variants
        DataStore dataStore = VariantStorageManager.getDataStore(catalogManager, study, File.Bioformat.VARIANT, sessionId);

        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        synchronizeCatalogStudyFromStorage(dataStore, study, sessionId);

        List<String> fileNames = new ArrayList<>(files.size());
        List<String> filePaths = new ArrayList<>(files.size());
        for (String fileStr : files) {
            File file = catalogManager.getFileManager().get(study, fileStr, null, sessionId).first();
            if (file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                fileNames.add(file.getName());
                filePaths.add(file.getPath());
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

        variantStorageEngine.removeFiles(study, fileNames);


        // Update study configuration to synchronize
        synchronizeCatalogStudyFromStorage(dataStore, study, sessionId);

        return catalogManager.getFileManager().search(study,
                new Query(FileDBAdaptor.QueryParams.PATH.key(), filePaths), new QueryOptions(), sessionId).getResults();
    }

    public void removeStudy(String study, QueryOptions options, String sessionId)
            throws CatalogException, StorageEngineException, IOException {

        // We get the credentials of the Datastore to insert the variants
        DataStore dataStore = VariantStorageManager.getDataStore(catalogManager, study, File.Bioformat.VARIANT, sessionId);

        // Update study configuration BEFORE executing the operation and fetching files from Catalog
        synchronizeCatalogStudyFromStorage(dataStore, study, sessionId);

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
        variantStorageEngine.getOptions().putAll(options);

        variantStorageEngine.removeStudy(study);


        // Update study configuration to synchronize
        synchronizeCatalogStudyFromStorage(dataStore, study, sessionId);

    }

}
