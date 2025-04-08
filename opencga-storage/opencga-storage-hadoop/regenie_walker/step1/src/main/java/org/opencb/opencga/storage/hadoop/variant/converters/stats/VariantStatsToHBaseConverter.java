/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.converters.stats;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper.Column;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.generateVariantRowKey;

/**
 * Created on 07/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsToHBaseConverter extends AbstractPhoenixConverter implements Converter<VariantStatsWrapper, Put> {

    private final int studyId;
    private final Logger logger = LoggerFactory.getLogger(VariantStatsToHBaseConverter.class);
    private final Map<String, Integer> cohortIds;

    public VariantStatsToHBaseConverter(StudyMetadata studyMetadata, Map<String, Integer> cohortIds) {
        super(GenomeHelper.COLUMN_FAMILY_BYTES);
        this.studyId = studyMetadata.getId();
        this.cohortIds = cohortIds;
    }

    @Override
    public Put convert(VariantStatsWrapper variantStatsWrapper) {
        if (variantStatsWrapper.getCohortStats() == null || variantStatsWrapper.getCohortStats().isEmpty()) {
            return null;
        }

        byte[] row = generateVariantRowKey(
                variantStatsWrapper.getChromosome(), variantStatsWrapper.getStart(), variantStatsWrapper.getEnd(),
                variantStatsWrapper.getReference(), variantStatsWrapper.getAlternate(), variantStatsWrapper.getSv());
        Put put = new Put(row);
        for (VariantStats stats : variantStatsWrapper.getCohortStats()) {
            Integer cohortId = cohortIds.get(stats.getCohortId());
            if (cohortId == null) {
                continue;
            }
            Column mafColumn = VariantPhoenixSchema.getStatsMafColumn(studyId, cohortId);
            Column mgfColumn = VariantPhoenixSchema.getStatsMgfColumn(studyId, cohortId);
            Column passFreqColumn = VariantPhoenixSchema.getStatsPassFreqColumn(studyId, cohortId);
            Column cohortColumn = VariantPhoenixSchema.getStatsFreqColumn(studyId, cohortId);
            Column statsColumn = VariantPhoenixSchema.getStatsColumn(studyId, cohortId);

            add(put, mafColumn, stats.getMaf());
            add(put, mgfColumn, stats.getMgf());
            add(put, cohortColumn, Arrays.asList(stats.getRefAlleleFreq(), stats.getAltAlleleFreq()));
            add(put, passFreqColumn, stats.getFilterFreq().getOrDefault(VCFConstants.PASSES_FILTERS_v4, 0F));

            VariantProto.VariantStats.Builder builder = VariantProto.VariantStats.newBuilder()
                    .setSampleCount(stats.getSampleCount())
                    .setFileCount(stats.getFileCount())
                    .setAltAlleleFreq(stats.getAltAlleleFreq())
                    .setAltAlleleCount(stats.getAltAlleleCount())
                    .setRefAlleleFreq(stats.getRefAlleleFreq())
                    .setRefAlleleCount(stats.getRefAlleleCount())
                    .setAlleleCount(stats.getAlleleCount())
                    .setMissingAlleleCount(stats.getMissingAlleleCount())
                    .setMissingGenotypeCount(stats.getMissingGenotypeCount());

            if (stats.getMafAllele() != null) {
                if (stats.getMafAllele().isEmpty()) {
                    // Proto does not allow null values.
                    // proto | avro
                    // ""    | null
                    // "-"   | ""
                    builder.setMafAllele("-");
                } else {
                    builder.setMafAllele(stats.getMafAllele());
                }
            }
            builder.setMaf(stats.getMaf());

            if (stats.getMgfGenotype() != null) {
                builder.setMgfGenotype(stats.getMgfGenotype());
            }
            builder.setMgf(stats.getMgf());

            if (stats.getGenotypeCount() != null) {
                builder.putAllGenotypeCount(stats.getGenotypeCount());
//                assert builder.getGenotypeCount() == stats.getGenotypeCount().size();
            }

            if (stats.getGenotypeFreq() != null) {
                builder.putAllGenotypeFreq(stats.getGenotypeFreq());
//                assert builder.getGenotypeFreqCount() == stats.getGenotypeFreq().size();
            }

            if (stats.getFilterCount() != null) {
                builder.putAllFilterCount(stats.getFilterCount());
                builder.putAllFilterFreq(stats.getFilterFreq());
            }

            if (stats.getQualityAvg() != null) {
                builder.setQualityCount(stats.getQualityCount());
                builder.setQualityAvg(stats.getQualityAvg());
            }

            add(put, statsColumn, builder.build().toByteArray());
        }
        return put;
    }

}
