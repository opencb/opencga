package org.opencb.opencga.app.migrations.v2_1_0.catalog.java;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileQualityControl;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.variant.VariantFileQualityControl;

@Migration(id="move_variant_file_stats_to_qc", description = "Move opencga_file_variant_stats annotation set from variable sets to FileQualityControl", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 1,
        rank = 9)
public class VariantFileStatsRelocation extends MigrationTool {

    private static final String FILE_VARIANT_STATS_VARIABLE_SET = "opencga_file_variant_stats";

    @Override
    protected void run() throws Exception {
        for (Study study : catalogManager.getStudyManager().search(new Query(), new QueryOptions(), token).getResults()) {
            Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.VCF);
            QueryOptions options = new QueryOptions();

            ObjectMapper objectMapper = new ObjectMapper(new JsonFactory());
            logger.info("Updating files from study {}", study.getFqn());
            try (DBIterator<File> it = catalogManager.getFileManager().iterator(study.getFqn(), query, options, token)) {
                while (it.hasNext()) {
                    File file = it.next();
                    if (file.getQualityControl() != null
                            && file.getQualityControl().getVariant() != null
                            && file.getQualityControl().getVariant().getVariantSetMetrics() != null) {
                        logger.info("Variant stats from file {} already relocated.", file.getId());
                        continue;
                    }
                    VariantSetStats variantSetStats = null;
                    for (AnnotationSet annotationSet : file.getAnnotationSets()) {
                        if (annotationSet.getVariableSetId().equals(FILE_VARIANT_STATS_VARIABLE_SET)) {
                            variantSetStats = objectMapper.convertValue(annotationSet.getAnnotations(), VariantSetStats.class);
                        }
                    }
                    if (variantSetStats != null) {
                        logger.info("Relocating variant stats from file {}", file.getId());
                        catalogManager.getFileManager().update(study.getFqn(), file.getId(),
                                new FileUpdateParams().setQualityControl(
                                        new FileQualityControl().setVariant(new VariantFileQualityControl(variantSetStats))),
                                new QueryOptions(), token);
                    }
                }
            }
        }
    }
}
