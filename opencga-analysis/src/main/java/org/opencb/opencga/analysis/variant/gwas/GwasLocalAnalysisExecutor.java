/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant.gwas;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.tools.variant.GwasAnalysisExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.plain.StringDataWriter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.oskar.analysis.stats.FisherExactTest;
import org.opencb.oskar.analysis.stats.FisherTestResult;

import java.util.*;
import java.util.concurrent.ExecutionException;

@ToolExecutor(id = "opencga-local",
        tool = GwasAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public final class GwasLocalAnalysisExecutor extends GwasAnalysisExecutor implements VariantStorageToolExecutor {

    @Override
    public void run() throws ToolException {
        List<String> sampleList1 = getSampleList1();
        List<String> sampleList2 = getSampleList2();
        List<String> allSamples = new ArrayList<>();
        allSamples.addAll(sampleList1);
        allSamples.addAll(sampleList2);

        try {
            Query query = new Query(VariantQueryParam.STUDY.key(), getStudy())
                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), allSamples);
            VariantDBReader reader = new VariantDBReader(getVariantStorageManager().iterator(query, new QueryOptions(), getToken()));

            ProgressLogger progressLogger = new ProgressLogger("Processed variants:");

            Task<Variant, String> task = Task.forEach(this::computeFisherTest)
                    .then((Task<String, String>) batch -> {
                progressLogger.increment(batch.size());
                return batch;
            });

            // TODO: Write header
            StringDataWriter writer = new StringDataWriter(getOutputFile(), true);

            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                    .setNumTasks(4)
                    .setBatchSize(100)
                    .setSorted(true)
                    .build();
            ParallelTaskRunner<Variant, String> ptr = new ParallelTaskRunner<>(reader, task, writer, config);

            ptr.run();
        } catch (CatalogException | ExecutionException | StorageEngineException e) {
            throw new ToolException(e);
        }
    }

    private String computeFisherTest(Variant variant) {
        VariantStats caseStats = VariantStatsCalculator.calculate(variant, variant.getStudies().get(0), getSampleList1());
        VariantStats controlStats = VariantStatsCalculator.calculate(variant, variant.getStudies().get(0), getSampleList2());

        int a = caseStats.getRefAlleleCount(); // case #REF
        int b = controlStats.getRefAlleleCount(); // control #REF
        int c = caseStats.getAltAlleleCount(); // case #ALT
        int d = controlStats.getAltAlleleCount(); // control #ALT

        if (a + b + c + d == 0) {
            return null;
        }
        FisherTestResult fisherTestResult = new FisherExactTest().fisherTest(a, b, c, d);


        VariantAnnotation variantAnnotation = variant.getAnnotation();
        String id = null;
        Set<String> genes = Collections.emptySet();
        if (variantAnnotation != null) {
            id = variantAnnotation.getId();
            genes = new HashSet<>();
            if (variantAnnotation.getConsequenceTypes() != null) {
                for (ConsequenceType consequenceType : variantAnnotation.getConsequenceTypes()) {
                    if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                        genes.add(consequenceType.getGeneName());
                    }
                }
            }
        }
        if (StringUtils.isEmpty(id)) {
            id = variant.getId();
        }
        if (genes.isEmpty()) {
            genes = Collections.singleton(".");
        }

        return tsv(
                id,
                variant.toString(),
                variant.getChromosome(), variant.getStart(),
                variant.getReference().isEmpty() ? "-" : variant.getReference(),
                variant.getAlternate().isEmpty() ? "-" : variant.getAlternate(),
                String.join(",", genes),
                a, b, c, d,
                caseStats.getAlleleCount(),
                controlStats.getAlleleCount(),
                caseStats.getAltAlleleFreq(),
                controlStats.getAltAlleleFreq(),
                fisherTestResult.getpValue(),
                fisherTestResult.getOddRatio()
        );
    }

    private static String tsv(Object... objects) {
        StringJoiner joiner = new StringJoiner("\t");
        for (Object object : objects) {
            final String toString;
            if (object instanceof Double) {
                if (((Double) object).isNaN() || ((Double) object).isInfinite()) {
                    toString = "NA";
                } else {
                    toString = object.toString();
                }
            } else {
                toString = object.toString();
            }
            joiner.add(toString);
        }
        return joiner.toString();
    }
}
