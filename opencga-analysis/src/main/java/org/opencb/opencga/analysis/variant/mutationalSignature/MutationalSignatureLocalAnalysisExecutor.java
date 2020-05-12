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

package org.opencb.opencga.analysis.variant.mutationalSignature;

import htsjdk.samtools.reference.BlockCompressedIndexedFastaSequenceFile;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.GZIIndex;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.File;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor implements VariantStorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-r:2.0.0-dev";

    public final static String CONTEXT_FILENAME = "context.txt";

    @Override
    public void run() throws ToolException {
        Map<String, Map<String, Double>> countMap = initFreqMap();
        try {
            VariantStorageManager storageManager = getVariantStorageManager();

            // Compute signature profile: contextual frequencies of each type of base substitution

            Query query = new Query()
                    .append(VariantQueryParam.STUDY.key(), getStudy())
                    .append(VariantQueryParam.SAMPLE.key(), getSampleName())
                    //.append(VariantQueryParam.FILTER.key(), "PASS")
                    .append(VariantQueryParam.TYPE.key(), "SNV");

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "chromosome,start,end,reference,alternate");

            VariantDBIterator iterator = storageManager.iterator(query, queryOptions, getToken());

            // Read mutation context from reference genome (.gz, .gz.fai and .gz.gzi files)
            String base = getRefGenomePath().toAbsolutePath().toString();
            BlockCompressedIndexedFastaSequenceFile indexed = new BlockCompressedIndexedFastaSequenceFile(getRefGenomePath(),
                    new FastaSequenceIndex(new File(base + ".fai")), GZIIndex.loadIndex(Paths.get(base + ".gzi")));

            while (iterator.hasNext()) {
                Variant variant = iterator.next();
                String key = variant.getReference() + ">" + variant.getAlternate();
                if (countMap.containsKey(key)) {
                    try {
                        ReferenceSequence refSeq = indexed.getSubsequenceAt(variant.getChromosome(), variant.getStart() - 1,
                                variant.getEnd() + 1);
                        String sequence = new String(refSeq.getBases());

                        if (countMap.get(key).containsKey(sequence)) {
                            countMap.get(key).put(sequence, countMap.get(key).get(sequence) + 1);
                        }
                    } catch (Exception e) {
                        System.out.println("Error getting context sequence for variant " + variant.toStringSimple() + ": " + e.getMessage());
                    }
                }
            }

            // Write context
            writeCountMap(countMap, getOutDir().resolve(CONTEXT_FILENAME).toFile());

            // Execute R script in docker
            String rScriptPath = getExecutorParams().getString("opencgaHome") + "/analysis/R/" + getToolId();
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(rScriptPath, "/data/input"));
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    "/data/output");
            String scriptParams = "R CMD Rscript --vanilla /data/input/mutational-signature.r /data/output/" + CONTEXT_FILENAME + " "
                    + "/data/output/" + MutationalSignatureAnalysis.SIGNATURES_FILENAME + " /data/output";
            String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams, null);
            System.out.println("Docker command line: " + cmdline);
        } catch (Exception e) {
            throw new ToolExecutorException(e);
        }


        // Check output files
        if (!new File(getOutDir() + "/signature_summary.png").exists()
                || !new File(getOutDir() + "/signature_coefficients.json").exists()) {
            String msg = "Something wrong executing mutational signature.";
            throw new ToolException(msg);
        }
    }
}
