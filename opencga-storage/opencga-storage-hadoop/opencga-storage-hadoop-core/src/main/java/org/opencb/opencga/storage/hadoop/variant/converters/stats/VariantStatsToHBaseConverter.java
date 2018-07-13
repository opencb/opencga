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

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.Converter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper.Column;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory.generateVariantRowKey;

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
        super(genomeHelper.getColumnFamily());
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
        byte[] row = generateVariantRowKey(
                variantStatsWrapper.getChromosome(), variantStatsWrapper.getStart(),
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
                builder.setMafAllele(stats.getMafAllele());
            }
            builder.setMaf(stats.getMaf());

            if (stats.getMgfGenotype() != null) {
                builder.setMgfGenotype(stats.getMgfGenotype());
            }
            builder.setMgf(stats.getMgf());

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

}
