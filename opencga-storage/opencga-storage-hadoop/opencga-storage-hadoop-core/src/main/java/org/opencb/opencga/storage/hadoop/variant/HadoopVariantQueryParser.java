package org.opencb.opencga.storage.hadoop.variant;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;

import java.util.List;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

public class HadoopVariantQueryParser extends VariantQueryParser {
    public HadoopVariantQueryParser(CellBaseUtils cellBaseUtils, VariantStorageMetadataManager metadataManager) {
        super(cellBaseUtils, metadataManager);
    }

    @Override
    protected Query preProcessQuery(Query originalQuery, QueryOptions options, VariantQueryProjection projection) {
        Query query = super.preProcessQuery(originalQuery, options, projection);
        List<String> studyNames = metadataManager.getStudyNames();

        if (isValidParam(query, STUDY) && studyNames.size() == 1) {
            String study = query.getString(STUDY.key());
            if (!isNegated(study)) {
                // Check that study exists
                metadataManager.getStudyId(study);
                query.remove(STUDY.key());
            }
        }

        convertGenesToRegionsQuery(query, cellBaseUtils);
        return query;
    }
}
