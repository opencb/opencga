package org.opencb.opencga.app.migrations;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.migration.MigrationException;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.*;

public abstract class StorageMigrationTool extends MigrationTool {

    private VariantStorageManager variantStorageManager;
    private StorageEngineFactory engineFactory;

    protected final VariantStorageManager getVariantStorageManager() throws MigrationException {
        if (variantStorageManager == null) {
            variantStorageManager = new VariantStorageManager(catalogManager, getVariantStorageEngineFactory());
        }
        return variantStorageManager;
    }

    public StorageEngineFactory getVariantStorageEngineFactory() throws MigrationException {
        if (engineFactory == null) {
            engineFactory = StorageEngineFactory.get(readStorageConfiguration());
        }
        return engineFactory;
    }

    protected final VariantStorageEngine getVariantStorageEngineByProject(String projectFqn) throws Exception {
        DataStore dataStore = getVariantStorageManager().getDataStoreByProjectId(projectFqn, token);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngineFactory()
                .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
        if (dataStore.getOptions() != null) {
            variantStorageEngine.getOptions().putAll(dataStore.getOptions());
        }
        return variantStorageEngine;
    }

    /**
     * Get list of projects that exist at the VariantStorage.
     * @return List of projects
     * @throws Exception on error
     */
    protected final List<String> getVariantStorageProjects() throws Exception {
        Set<String> projects = new LinkedHashSet<>();

        for (String studyFqn : getVariantStorageStudies()) {
            projects.add(catalogManager.getStudyManager().getProjectFqn(studyFqn));
        }

        return new ArrayList<>(projects);
    }

    /**
     * Get list of studies that exist at the VariantStorage.
     * @return List of projects
     * @throws Exception on error
     */
    protected final List<String> getVariantStorageStudies() throws Exception {
        Set<String> studies = new LinkedHashSet<>();
        VariantStorageManager variantStorageManager = getVariantStorageManager();
        for (Study study : catalogManager.getStudyManager().search(new Query(), new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList("fqn")), token).getResults()) {
            if (variantStorageManager.exists(study.getFqn(), token)) {
                studies.add(study.getFqn());
            }
        }
        return new ArrayList<>(studies);
    }



}
