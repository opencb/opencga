package org.opencb.opencga.storage.hadoop.variant.index.stats;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 07/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsToHBaseConverter extends AbstractPhoenixConverter implements Converter<VariantStatsWrapper, Put> {

    private final GenomeHelper genomeHelper;
    private final StudyConfiguration studyConfiguration;
    private final int studyId;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsToHBaseConverter.class);

    public VariantStatsToHBaseConverter(GenomeHelper genomeHelper, StudyConfiguration studyConfiguration) {
        this.genomeHelper = genomeHelper;
        this.studyConfiguration = studyConfiguration;
        this.studyId = studyConfiguration.getStudyId();
    }

    @Override
    public Put convert(VariantStatsWrapper variantStatsWrapper) {
        if (variantStatsWrapper.getCohortStats() == null || variantStatsWrapper.getCohortStats().isEmpty()) {
            return null;
        }

        VariantStats firstStats = variantStatsWrapper.getCohortStats().entrySet().iterator().next().getValue();
        byte[] row = genomeHelper.generateVariantRowKey(
                variantStatsWrapper.getChromosome(), variantStatsWrapper.getPosition(),
                firstStats.getRefAllele(), firstStats.getAltAllele());
        Put put = new Put(row);
        for (Map.Entry<String, VariantStats> entry : variantStatsWrapper.getCohortStats().entrySet()) {
            Integer cohortId = studyConfiguration.getCohortIds().get(entry.getKey());
            Column mafColumn = VariantPhoenixHelper.getMafColumn(studyId, cohortId);
            Column mgfColumn = VariantPhoenixHelper.getMgfColumn(studyId, cohortId);
            Column statsColumn = VariantPhoenixHelper.getStatsColumn(studyId, cohortId);

            VariantStats stats = entry.getValue();
            add(put, mafColumn, stats.getMaf());
            add(put, mgfColumn, stats.getMgf());

            VariantProto.VariantStats.Builder builder = VariantProto.VariantStats.newBuilder()
                    .setAltAlleleFreq(stats.getAltAlleleFreq())
                    .setAltAlleleCount(stats.getAltAlleleCount())
                    .setRefAlleleFreq(stats.getRefAlleleFreq())
                    .setRefAlleleCount(stats.getRefAlleleCount())
                    .setMissingAlleles(stats.getMissingAlleles())
                    .setMissingGenotypes(stats.getMissingGenotypes());

            if (stats.getMafAllele() != null) {
                builder.setMaf(stats.getMaf())
                        .setMafAllele(stats.getMafAllele());
            }

            if (stats.getMgfGenotype() != null) {
                builder.setMgf(stats.getMgf())
                        .setMgfGenotype(stats.getMgfGenotype());
            }

            if (stats.getGenotypesCount() != null) {
                Map<String, Integer> map = new HashMap<>(stats.getGenotypesCount().size());
                stats.getGenotypesCount().forEach((genotype, count) -> map.put(genotype.toString(), count));
                builder.putAllGenotypesCount(map);
            }

            if (stats.getGenotypesFreq() != null) {
                Map<String, Float> map = new HashMap<>(stats.getGenotypesFreq().size());
                stats.getGenotypesFreq().forEach((genotype, freq) -> map.put(genotype.toString(), freq));
                builder.putAllGenotypesFreq(map);
            }

            add(put, statsColumn, builder.build().toByteArray());
        }
        return put;
    }

    @Override
    protected GenomeHelper getGenomeHelper() {
        return genomeHelper;
    }
}
