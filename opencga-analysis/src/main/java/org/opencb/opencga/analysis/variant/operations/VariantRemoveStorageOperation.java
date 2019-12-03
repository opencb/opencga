package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections.CollectionUtils;
import org.apache.solr.common.StringUtils;
import org.opencb.opencga.analysis.variant.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.FileIndex;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created on 07/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Analysis(id = "variant-file-remove", type = Analysis.AnalysisType.VARIANT)
public class VariantRemoveStorageOperation extends StorageOperation {

    private String study;
    private boolean removeWholeStudy;
    private List<String> files;

    public VariantRemoveStorageOperation setStudy(String study) {
        this.study = study;
        return this;
    }

    public VariantRemoveStorageOperation setRemoveWholeStudy(boolean removeWholeStudy) {
        this.removeWholeStudy = removeWholeStudy;
        return this;
    }

    public VariantRemoveStorageOperation setFiles(List<String> files) {
        this.files = files;
        return this;
    }


    @Override
    protected void check() throws Exception {
        super.check();
        if (StringUtils.isEmpty(study)) {
            throw new AnalysisException("Missing study");
        }
        if (removeWholeStudy && CollectionUtils.isNotEmpty(files)) {
            throw new AnalysisException("Colliding params 'removeWholeStudy' and list of files. Can not mix them.");
        }
        if (!removeWholeStudy && CollectionUtils.isEmpty(files)) {
            throw new AnalysisException("Missing list of files to remove");
        }

        study = catalogManager.getStudyManager().resolveId(study, catalogManager.getUserManager().getUserId(token)).getFqn();

    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList("updateStorageMetadata", getId());
    }

    @Override
    protected void run() throws Exception {
        // We get the credentials of the Datastore to insert the variants
        DataStore dataStore = VariantStorageManager.getDataStore(catalogManager, study, File.Bioformat.VARIANT, token);

        List<String> fileNames = new ArrayList<>();
//        List<String> filePaths = new ArrayList<>(files.size());
        step("updateStorageMetadata", () -> {
            // Update study configuration BEFORE executing the operation and fetching files from Catalog
            synchronizeCatalogStudyFromStorage(dataStore, study, token);

            if (!removeWholeStudy) {
                for (String fileStr : files) {
                    File file = catalogManager.getFileManager().get(study, fileStr, null, token).first();
                    if (file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                        fileNames.add(file.getName());
//                        filePaths.add(file.getPath());
                    } else {
                        throw new CatalogException("Unable to remove variants from file " + file.getName() + ". "
                                + "IndexStatus = " + file.getIndex().getStatus().getName());
                    }
                }

                if (fileNames.isEmpty()) {
                    throw new CatalogException("Nothing to do!");
                }
            }
        });

        step(getId(), () -> {
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
            variantStorageEngine.getOptions().putAll(params);

            if (removeWholeStudy) {
                variantStorageEngine.removeStudy(study);
                new CatalogStorageMetadataSynchronizer(catalogManager, variantStorageEngine.getMetadataManager())
                        .synchronizeRemovedStudyFromStorage(study, token);
            } else {
                variantStorageEngine.removeFiles(study, fileNames);
                // Update study configuration to synchronize
                synchronizeCatalogStudyFromStorage(dataStore, study, token);
            }

        });

    }

}
