package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.LinkedList;
import java.util.List;

@Analysis(id = VariantFamilyIndexStorageOperation.ID, type = Analysis.AnalysisType.VARIANT)
public class VariantFamilyIndexStorageOperation extends StorageOperation {


    public static final String ID = "variant-family-index";
    private String study;
    private List<String> familiesStr;
    private boolean skipIncompleteFamily;
    private boolean overwrite;

    public VariantFamilyIndexStorageOperation setStudy(String study) {
        this.study = study;
        return this;
    }

    public VariantFamilyIndexStorageOperation setFamily(List<String> family) {
        this.familiesStr = family;
        return this;
    }

    public VariantFamilyIndexStorageOperation setSkipIncompleteFamily(boolean skipIncompleteFamily) {
        this.skipIncompleteFamily = skipIncompleteFamily;
        return this;
    }

    public VariantFamilyIndexStorageOperation setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        skipIncompleteFamily = params.getBoolean("skipIncompleteFamily", skipIncompleteFamily);
        overwrite = params.getBoolean("overwrite", overwrite);

        study = getStudyFqn(study);

        if (CollectionUtils.isEmpty(familiesStr)) {
            throw new IllegalArgumentException("Empty list of families");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            VariantStorageEngine engine = getVariantStorageEngine(study);

            List<List<String>> trios = new LinkedList<>();

            VariantStorageMetadataManager metadataManager = engine.getMetadataManager();
            VariantCatalogQueryUtils catalogUtils = new VariantCatalogQueryUtils(catalogManager);
            if (familiesStr.size() == 1 && familiesStr.get(0).equals(VariantQueryUtils.ALL)) {
                DBIterator<Family> iterator = catalogManager.getFamilyManager().iterator(study, new Query(), new QueryOptions(), token);
                while (iterator.hasNext()) {
                    Family family = iterator.next();
                    trios.addAll(catalogUtils.getTriosFromFamily(study, family, metadataManager, true, token));
                }
            } else {
                for (String familyId : familiesStr) {
                    Family family = catalogManager.getFamilyManager().get(study, familyId, null, token).first();
                    trios.addAll(catalogUtils.getTriosFromFamily(study, family, metadataManager, skipIncompleteFamily, token));
                }
            }

            engine.familyIndex(study, trios, params);
        });
    }
}
