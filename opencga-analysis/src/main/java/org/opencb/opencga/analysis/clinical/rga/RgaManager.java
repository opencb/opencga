package org.opencb.opencga.analysis.clinical.rga;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.config.Catalog;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.rga.RgaEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class RgaManager extends StorageManager {

    private final RgaEngine rgaEngine;

    public RgaManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);
        this.rgaEngine = new RgaEngine(getStorageConfiguration());
    }

    public RgaManager(Configuration configuration, StorageConfiguration storageConfiguration) throws CatalogException {
        super(configuration, storageConfiguration);
        this.rgaEngine = new RgaEngine(storageConfiguration);
    }

    public OpenCGAResult<KnockoutByIndividual> individualQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        // TODO: Check Permissions
        String collection = getCollectionName(study.getFqn());
        return rgaEngine.individualQuery(collection, query, options);
    }

    public OpenCGAResult<KnockoutByGene> geneQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        // TODO: Check Permissions
        String collection = getCollectionName(study.getFqn());
        return rgaEngine.geneQuery(collection, query, options);
    }

    public void index(String studyStr, String fileStr, String token) throws CatalogException, RgaException, IOException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        try {
            catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error(e.getMessage(), e);
            throw new CatalogException("Only owners or admins can index", e.getCause());
        }

        File file = catalogManager.getFileManager().get(studyStr, fileStr, FileManager.INCLUDE_FILE_URI_PATH, token).first();

        String collectionName = getCollectionName(study.getFqn());
        index(collectionName, Paths.get(file.getUri()));
    }

    private void index(String collection, Path path) throws RgaException, IOException {
        try {
            if (!rgaEngine.exists(collection)) {
                rgaEngine.create(collection);
            }
        } catch (RgaException e) {
            logger.error("Could not perform RGA index in collection {}", collection, e);
            throw new RgaException("Could not perform RGA index in collection '" + collection + "'.");
        }

        rgaEngine.load(collection, path);
    }

    @Override
    public void testConnection() throws StorageEngineException {
        rgaEngine.isAlive("test");
    }

    private String getCollectionName(String study) {
        return catalogManager.getConfiguration().getDatabasePrefix() + "-rga-" + study.replace("@", "_").replace(":", "_");
    }
}
