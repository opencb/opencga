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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.opencb.biodata.models.clinical.qc.MutationalSignature;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.clinical.qc.SignatureFitting;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis.SIGNATURES_FILENAME;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor implements StorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-r:" + GitRepositoryState.get().getBuildVersion();

    public final static String CONTEXT_FILENAME = "context.txt";

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private VariantStorageManager storageManager;

    public MutationalSignatureLocalAnalysisExecutor() {
    }

    public MutationalSignatureLocalAnalysisExecutor(VariantStorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public VariantStorageManager getVariantStorageManager() throws ToolExecutorException {
        if (storageManager == null) {
            storageManager = StorageToolExecutor.super.getVariantStorageManager();
        }
        return storageManager;
    }

    @Override
    public void run() throws ToolException, IOException {
        // Context index filename
        File indexFile = getOutDir().resolve(getContextIndexFilename(getSampleName())).toFile();
        PrintWriter pw = new PrintWriter(indexFile);

        try {
            // Compute signature profile: contextual frequencies of each type of base substitution

            Query query = new Query()
                    .append(VariantQueryParam.STUDY.key(), getStudy())
                    .append(VariantQueryParam.SAMPLE.key(), getSampleName())
                    .append(VariantQueryParam.TYPE.key(), VariantType.SNV);

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id");

            // Get variant iterator
            VariantDBIterator iterator = getVariantIterator(query, queryOptions);

            // Read mutation context from reference genome (.gz, .gz.fai and .gz.gzi files)
            String base = getRefGenomePath().toAbsolutePath().toString();
            BlockCompressedIndexedFastaSequenceFile indexed = new BlockCompressedIndexedFastaSequenceFile(getRefGenomePath(),
                    new FastaSequenceIndex(new File(base + ".fai")), GZIIndex.loadIndex(Paths.get(base + ".gzi")));

            Map<String, Map<String, Double>> countMap = initFreqMap();

            while (iterator.hasNext()) {
                Variant variant = iterator.next();

                // Accessing to the context sequence and write it into the context index file
                ReferenceSequence refSeq = indexed.getSubsequenceAt(variant.getChromosome(), variant.getStart() - 1,
                        variant.getEnd() + 1);
                String sequence = new String(refSeq.getBases());

                // Write context index
                pw.println(variant.toString() + "\t" + sequence);

                // Update count map
                updateCountMap(variant, sequence, countMap);
            }

            // Write context counts
            writeCountMap(countMap, getOutDir().resolve(CONTEXT_FILENAME).toFile());

            // Close context index file
            pw.close();

            // Execute R script in docker
            executeRScript();
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

    public MutationalSignature query(Query query, QueryOptions queryOptions)
            throws CatalogException, ToolException, StorageEngineException, IOException {

        File signatureFile = ResourceUtils.downloadAnalysis(MutationalSignatureAnalysis.ID, SIGNATURES_FILENAME, getOutDir(),
                getOpenCgaHome());
        if (signatureFile == null) {
            throw new ToolException("Error downloading mutational signatures file from " + ResourceUtils.URL);
        }
        setMutationalSignaturePath(signatureFile.toPath());

        // Get context index filename
        String name = getContextIndexFilename(getSampleName());
        Query fileQuery = new Query("name", name);
        QueryOptions fileQueryOptions = new QueryOptions("include", "uri");
        OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult = getVariantStorageManager().getCatalogManager()
                .getFileManager().search(getStudy(), fileQuery, fileQueryOptions, getToken());

        if (CollectionUtils.isEmpty(fileResult.getResults())) {
            throw new ToolException("Missing mutational signature context index file for sample " + getSampleName() + " in catalog");
        }

        File indexFile = null;
        long maxSize = 0;
        for (org.opencb.opencga.core.models.file.File file : fileResult.getResults()) {
            File auxFile = new File(file.getUri().getPath());
            if (auxFile.exists() && auxFile.length() > maxSize) {
                maxSize = auxFile.length();
                indexFile = auxFile;
            }
        }
        if (indexFile == null) {
            throw new ToolException("Missing mutational signature context index file for sample " + getSampleName());
        }

        // Read context index
        long start = System.currentTimeMillis();
        Map<String, String> indexMap = new HashMap<>();
        BufferedReader br = new BufferedReader( new FileReader(indexFile));
        String line;
        while ( (line = br.readLine()) != null ){
            String[] parts = line.split("\t");
            indexMap.put(parts[0], parts[1]);
        }

        // Get variant iterator
        query.append(VariantQueryParam.TYPE.key(), VariantType.SNV);
        queryOptions.append(QueryOptions.INCLUDE, "id");
        VariantDBIterator iterator = getVariantIterator(query, queryOptions);

        Map<String, Map<String, Double>> countMap = initFreqMap();

        while (iterator.hasNext()) {
            Variant variant = iterator.next();

            // Update count map
            updateCountMap(variant, indexMap.get(variant.toString()), countMap);
        }

        // Write context counts
        writeCountMap(countMap, getOutDir().resolve(CONTEXT_FILENAME).toFile());

        // Run R script
        if (getExecutorParams().getBoolean("fitting")) {
            executeRScript();
        }

        return parse(getOutDir());
    }
    
    private void updateCountMap(Variant variant, String sequence, Map<String, Map<String, Double>> countMap) {
        String k, seq;

        String key = variant.getReference() + ">" + variant.getAlternate();

        if (countMap.containsKey(key)) {
            k = key;
            seq = sequence;
        } else {
            k = MutationalSignatureAnalysisExecutor.complement(key);
            seq = MutationalSignatureAnalysisExecutor.reverseComplement(sequence);
        }
        if (countMap.get(k).containsKey(seq)) {
            countMap.get(k).put(seq, countMap.get(k).get(seq) + 1);
        } else {
            logger.error("Something wrong happened counting mutational signature substitutions: variant = " + variant.toString()
                    + ", key = " + key + ", k = " + k + ", sequence = " + sequence + ", seq = " + seq);
        }
    }

    private VariantDBIterator getVariantIterator(Query query, QueryOptions queryOptions) throws ToolExecutorException, CatalogException,
            StorageEngineException {
        VariantStorageManager storageManager = getVariantStorageManager();

        // Compute signature profile: contextual frequencies of each type of base substitution
        return storageManager.iterator(query, queryOptions, getToken());
    }

    private String executeRScript() throws IOException {
        String rScriptPath = getExecutorParams().getString("opencgaHome") + "/analysis/R/" + getToolId();
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(rScriptPath, "/data/input"));
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                "/data/output");
        String scriptParams = "R CMD Rscript --vanilla /data/input/mutational-signature.r /data/output/" + CONTEXT_FILENAME + " "
                + "/data/output/" + SIGNATURES_FILENAME + " /data/output ";

        String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams, null);
        logger.info("Docker command line: " + cmdline);

        return cmdline;
    }

    private MutationalSignature parse(Path dir) throws IOException {
        MutationalSignature result = new MutationalSignature();

        // Context counts
        File contextFile = dir.resolve("context.txt").toFile();
        if (contextFile.exists()) {
            List<String> lines = FileUtils.readLines(contextFile, Charset.defaultCharset());
            Signature.SignatureCount[] sigCounts = new Signature.SignatureCount[lines.size() - 1];
            for (int i = 1; i < lines.size(); i++) {
                String[] fields = lines.get(i).split("\t");
                sigCounts[i-1] = new Signature.SignatureCount(fields[2], Math.round(Float.parseFloat((fields[3]))));
            }
            result.setSignature(new Signature("SNV", sigCounts));
        }

        // Signatures coefficients
        File coeffsFile = dir.resolve("signature_coefficients.json").toFile();
        if (coeffsFile.exists()) {
            SignatureFitting fitting = new SignatureFitting()
                    .setMethod("GEL")
                    .setSignatureSource("Cosmic")
                    .setSignatureVersion("2.0");

            Map content = JacksonUtils.getDefaultObjectMapper().readValue(coeffsFile, Map.class);
            Map coefficients = (Map) content.get("coefficients");
            SignatureFitting.Score[] scores = new SignatureFitting.Score[coefficients.size()];
            int i = 0;
            for (Object key : coefficients.keySet()) {
                Number coeff = (Number) coefficients.get(key);
                scores[i++] = new SignatureFitting.Score((String) key, coeff.doubleValue());
            }
            fitting.setScores(scores);
            fitting.setCoeff((Double) content.get("rss"));

            // Signature summary image
            File imgFile = dir.resolve("signature_summary.png").toFile();
            if (imgFile.exists()) {
                FileInputStream fileInputStreamReader = new FileInputStream(imgFile);
                byte[] bytes = new byte[(int) imgFile.length()];
                fileInputStreamReader.read(bytes);

                fitting.setImage(new String(Base64.getEncoder().encode(bytes), "UTF-8"));
            }

            result.setFitting(fitting);
        }

        return result;
    }
}
