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

package org.opencb.opencga.storage.hadoop.variant.stats;

import com.google.common.collect.BiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsCalculator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.VariantStatsToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.mr.AbstractHBaseVariantMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 07/12/2016.
 */
@Deprecated
public class AnalysisStatsMapper extends AbstractHBaseVariantMapper<ImmutableBytesWritable, Put> {

    private Logger logger = LoggerFactory.getLogger(AnalysisStatsMapper.class);
    private VariantStatisticsCalculator variantStatisticsCalculator;
    private String studyId;
    private Map<String, Set<String>> samples;
    private VariantStatsToHBaseConverter variantStatsToHBaseConverter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.getHbaseToVariantConverter().setSimpleGenotypes(true);
        variantStatisticsCalculator = new VariantStatisticsCalculator(true);
        this.variantStatisticsCalculator.setAggregationType(Aggregation.NONE, null);
        this.studyId = Integer.valueOf(this.getStudyConfiguration().getStudyId()).toString();
        BiMap<Integer, String> sampleIds = getStudyConfiguration().getSampleIds().inverse();
        variantStatsToHBaseConverter = new VariantStatsToHBaseConverter(this.getHelper(), this.getStudyConfiguration());
        // map from cohort Id to <cohort name, <sample names>>
        this.samples = this.getStudyConfiguration().getCohortIds().entrySet().stream()
                .map(e -> new MutablePair<>(e.getKey(), this.getStudyConfiguration().getCohorts().get(e.getValue())))
                .map(p -> new MutablePair<>(p.getKey(),
                        p.getValue().stream().map(sampleIds::get).collect(Collectors.toSet())))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        this.samples.forEach((cohortId, samples) ->
                logger.info("Calculate {} stats for cohort {} with {}", studyId, cohortId, StringUtils.join(samples, ",")));
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        boolean done = false;
        try {
            Variant variant = this.getHbaseToVariantConverter().convert(value);
            List<VariantStatsWrapper> annotations = this.variantStatisticsCalculator.calculateBatch(
                    Collections.singletonList(variant), this.studyId, this.samples);
            for (VariantStatsWrapper annotation : annotations) {
                Put convert = this.variantStatsToHBaseConverter.convert(annotation);
                if (null != convert) {
                    context.write(key, convert);
                    done = true;
                    context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stats.put").increment(1);
                }
            }
            if (done) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants").increment(1);
            }
        } catch (IllegalStateException e) {
            throw new IllegalStateException("Problem with row [hex:" + Bytes.toHex(key.copyBytes()) + "]", e);
        }
    }
}
