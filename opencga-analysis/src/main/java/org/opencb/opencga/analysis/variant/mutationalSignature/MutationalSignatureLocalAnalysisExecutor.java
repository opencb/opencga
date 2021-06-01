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
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis.GENOME_CONTEXT_FILENAME;
import static org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis.SIGNATURES_FILENAME;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor implements StorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-r:" + GitRepositoryState.get().getBuildVersion();

    private Path opencgaHome;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, CatalogException, IOException, StorageEngineException {
        opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));


        // Get first variant to check where the genome context is stored
        Query query = new Query();
        if (getQuery() != null) {
            query.putAll(getQuery());
        }
        query.append(VariantQueryParam.TYPE.key(), VariantType.SNV);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.INCLUDE, "id");
        queryOptions.append(QueryOptions.LIMIT, "1");

        VariantQueryResult<Variant> variantQueryResult = getVariantStorageManager().get(query, queryOptions, getToken());
        Variant variant = variantQueryResult.first();
        if (variant == null) {
            // Nothing to do
            addWarning("None variant found for that mutational signature query");
            return;
        }

        // Check if genome context is stored in the sample data
        if (false) {
            // Run mutational analysis taking into account that the genome context is stored in the the sample data
            computeFromSampleData();
        } else {
            // Run mutational analysis taking into account that the genome context is stored in an index file
            computeFromContextFile();
        }
    }

    private void computeFromSampleData() throws ToolExecutorException {
        // Get variant iterator
        Query query = new Query();
        if (getQuery() != null) {
            query.putAll(getQuery());
        }
        query.append(VariantQueryParam.TYPE.key(), VariantType.SNV);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.INCLUDE, "id");

        try {
            VariantDBIterator iterator = getVariantStorageManager().iterator(query, queryOptions, getToken());
            Map<String, Map<String, Double>> countMap = initFreqMap();

            while (iterator.hasNext()) {
                Variant variant = iterator.next();

                // Update count map
                String context = "";
                updateCountMap(variant, context, countMap);
            }

            // Write context counts
            writeCountMap(countMap, getOutDir().resolve(GENOME_CONTEXT_FILENAME).toFile());

            // Run R script
            if (isFitting()) {
                executeRScript();
            }

        } catch (CatalogException | StorageEngineException | ToolException | IOException e) {
            throw new ToolExecutorException(e);
        }
    }

    private void computeFromContextFile() throws ToolExecutorException {
        // Context index filename
        File indexFile = null;
        String name = getContextIndexFilename(getSample());
        try {
            Query fileQuery = new Query("name", name);
            QueryOptions fileQueryOptions = new QueryOptions("include", "uri");
            OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult = getVariantStorageManager().getCatalogManager()
                    .getFileManager().search(getStudy(), fileQuery, fileQueryOptions, getToken());

            long maxSize = 0;
            for (org.opencb.opencga.core.models.file.File file : fileResult.getResults()) {
                File auxFile = new File(file.getUri().getPath());
                if (auxFile.exists() && auxFile.length() > maxSize) {
                    maxSize = auxFile.length();
                    indexFile = auxFile;
                }
            }
        } catch (CatalogException e) {
            throw new ToolExecutorException(e);
        }

        if (indexFile == null) {
            // The genome context file does not exist, we have to create it !!!
            indexFile = getOutDir().resolve(getContextIndexFilename(getSample())).toFile();
            createGenomeContextFile(indexFile);
        }

        if (!indexFile.exists()) {
            throw new ToolExecutorException("Could not create the genome context index file for sample " + getSample());
        }

        try {
            // Read context index
            Map<String, String> indexMap = new HashMap<>();
            BufferedReader br = new BufferedReader(new FileReader(indexFile));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                indexMap.put(parts[0], parts[1]);
            }

            // Get variant iterator
            Query query = new Query();
            if (getQuery() != null) {
                query.putAll(getQuery());
            }
            query.append(VariantQueryParam.TYPE.key(), VariantType.SNV);

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id");

            VariantDBIterator iterator = getVariantStorageManager().iterator(query, queryOptions, getToken());

            Map<String, Map<String, Double>> countMap = initFreqMap();

            while (iterator.hasNext()) {
                Variant variant = iterator.next();

                // Update count map
                updateCountMap(variant, indexMap.get(variant.toString()), countMap);
            }

            // Write context counts
            writeCountMap(countMap, getOutDir().resolve(GENOME_CONTEXT_FILENAME).toFile());

            // Run R script
            if (isFitting()) {
                executeRScript();
            }

        } catch (IOException | CatalogException | StorageEngineException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    private void createGenomeContextFile(File indexFile) throws ToolExecutorException {
        try {
            // First,
            ResourceUtils.DownloadedRefGenome refGenome = ResourceUtils.downloadRefGenome(getAssembly(), getOutDir(), opencgaHome);

            if (refGenome == null) {
                throw new ToolExecutorException("Something wrong happened accessing reference genome, check local path and public repository");
            }

            Path refGenomePath = refGenome.getGzFile().toPath();

            // Compute signature profile: contextual frequencies of each type of base substitution

            Query query = new Query()
                    .append(VariantQueryParam.STUDY.key(), getStudy())
                    .append(VariantQueryParam.SAMPLE.key(), getSample())
                    .append(VariantQueryParam.TYPE.key(), VariantType.SNV);

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id");

            // Get variant iterator
            VariantDBIterator iterator = getVariantStorageManager().iterator(query, queryOptions, getToken());

            // Read mutation context from reference genome (.gz, .gz.fai and .gz.gzi files)
            String base = refGenomePath.toAbsolutePath().toString();
            BlockCompressedIndexedFastaSequenceFile indexed = new BlockCompressedIndexedFastaSequenceFile(refGenomePath,
                    new FastaSequenceIndex(new File(base + ".fai")), GZIIndex.loadIndex(Paths.get(base + ".gzi")));

            PrintWriter pw = new PrintWriter(indexFile);
            while (iterator.hasNext()) {
                Variant variant = iterator.next();

                // Accessing to the context sequence and write it into the context index file
                ReferenceSequence refSeq = indexed.getSubsequenceAt(variant.getChromosome(), variant.getStart() - 1,
                        variant.getEnd() + 1);
                String sequence = new String(refSeq.getBases());

                // Write context index
                pw.println(variant.toString() + "\t" + sequence);
            }

            // Close context index file
            pw.close();
        } catch (IOException | CatalogException | ToolException | StorageEngineException e) {
            throw new ToolExecutorException(e);
        }
    }

//        VariantDBIterator iterator = getVariantStorageManager().iterator(query, queryOptions, getToken());
//
//
//        Map<String, Map<String, Double>> countMap = initFreqMap();
//
//        while (iterator.hasNext()) {
//            Variant variant = iterator.next();
//
//
//
//
//
//
//            PrintWriter pw = new PrintWriter(indexFile);
//
//            try {
//                // Compute signature profile: contextual frequencies of each type of base substitution
//
//                Query query = new Query()
//                        .append(VariantQueryParam.STUDY.key(), getStudy())
//                        .append(VariantQueryParam.SAMPLE.key(), getSampleName())
//                        .append(VariantQueryParam.TYPE.key(), VariantType.SNV);
//
//                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id");
//
//                // Get variant iterator
//                VariantDBIterator iterator = getVariantIterator(query, queryOptions);
//
//                // Read mutation context from reference genome (.gz, .gz.fai and .gz.gzi files)
//                String base = getRefGenomePath().toAbsolutePath().toString();
//                BlockCompressedIndexedFastaSequenceFile indexed = new BlockCompressedIndexedFastaSequenceFile(getRefGenomePath(),
//                        new FastaSequenceIndex(new File(base + ".fai")), GZIIndex.loadIndex(Paths.get(base + ".gzi")));
//
//                Map<String, Map<String, Double>> countMap = initFreqMap();
//
//                while (iterator.hasNext()) {
//                    Variant variant = iterator.next();
//
//                    // Accessing to the context sequence and write it into the context index file
//                    ReferenceSequence refSeq = indexed.getSubsequenceAt(variant.getChromosome(), variant.getStart() - 1,
//                            variant.getEnd() + 1);
//                    String sequence = new String(refSeq.getBases());
//
//                    // Write context index
//                    pw.println(variant.toString() + "\t" + sequence);
//
//                    // Update count map
//                    updateCountMap(variant, sequence, countMap);
//                }
//
//                // Write context counts
//                writeCountMap(countMap, getOutDir().resolve(CONTEXT_FILENAME).toFile());
//
//                // Close context index file
//                pw.close();
//
//                // Execute R script in docker
//                executeRScript();
//            } catch (Exception e) {
//                throw new ToolExecutorException(e);
//            }
//
//            // Check output files
//            if (!new File(getOutDir() + "/signature_summary.png").exists()
//                    || !new File(getOutDir() + "/signature_coefficients.json").exists()) {
//                String msg = "Something wrong executing mutational signature.";
//                throw new ToolException(msg);
//            }
//        }
//
//        public MutationalSignature query(Query query, QueryOptions queryOptions)
//            throws CatalogException, ToolException, StorageEngineException, IOException {
//
//            File signatureFile = ResourceUtils.downloadAnalysis(MutationalSignatureAnalysis.ID, SIGNATURES_FILENAME, getOutDir(),
//                    getOpenCgaHome());
//            if (signatureFile == null) {
//                throw new ToolException("Error downloading mutational signatures file from " + ResourceUtils.URL);
//            }
//            setMutationalSignaturePath(signatureFile.toPath());
//
//            // Get context index filename
//            String name = getContextIndexFilename(getSampleName());
//            Query fileQuery = new Query("name", name);
//            QueryOptions fileQueryOptions = new QueryOptions("include", "uri");
//            OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult = getVariantStorageManager().getCatalogManager()
//                    .getFileManager().search(getStudy(), fileQuery, fileQueryOptions, getToken());
//
//            if (CollectionUtils.isEmpty(fileResult.getResults())) {
//                throw new ToolException("Missing mutational signature context index file for sample " + getSampleName() + " in catalog");
//            }
//
//            File indexFile = null;
//            long maxSize = 0;
//            for (org.opencb.opencga.core.models.file.File file : fileResult.getResults()) {
//                File auxFile = new File(file.getUri().getPath());
//                if (auxFile.exists() && auxFile.length() > maxSize) {
//                    maxSize = auxFile.length();
//                    indexFile = auxFile;
//                }
//            }
//            if (indexFile == null) {
//                throw new ToolException("Missing mutational signature context index file for sample " + getSampleName());
//            }
//
//            // Read context index
//            long start = System.currentTimeMillis();
//            Map<String, String> indexMap = new HashMap<>();
//            BufferedReader br = new BufferedReader( new FileReader(indexFile));
//            String line;
//            while ( (line = br.readLine()) != null ){
//                String[] parts = line.split("\t");
//                indexMap.put(parts[0], parts[1]);
//            }
//
//            // Get variant iterator
//            query.append(VariantQueryParam.TYPE.key(), VariantType.SNV);
//            queryOptions.append(QueryOptions.INCLUDE, "id");
//            VariantDBIterator iterator = getVariantIterator(query, queryOptions);
//
//            Map<String, Map<String, Double>> countMap = initFreqMap();
//
//            while (iterator.hasNext()) {
//                Variant variant = iterator.next();
//
//                // Update count map
//                updateCountMap(variant, indexMap.get(variant.toString()), countMap);
//            }
//
//            // Write context counts
//            writeCountMap(countMap, getOutDir().resolve(CONTEXT_FILENAME).toFile());
//
//            // Run R script
//            if (getExecutorParams().getBoolean("fitting")) {
//                executeRScript();
//            }
//
//            return parse(getOutDir());
//        }

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

        private String executeRScript() throws IOException, ToolExecutorException {
            // Download signature profiles
            File signatureFile = ResourceUtils.downloadAnalysis(MutationalSignatureAnalysis.ID, SIGNATURES_FILENAME, getOutDir(),
                    opencgaHome);
            if (signatureFile == null) {
                throw new ToolExecutorException("Error downloading mutational signatures file from " + ResourceUtils.URL);
            }

            String rScriptPath = opencgaHome + "/analysis/R/" + getToolId();
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(rScriptPath, "/data/input"));
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    "/data/output");
            String scriptParams = "R CMD Rscript --vanilla /data/input/mutational-signature.r /data/output/" + GENOME_CONTEXT_FILENAME + " "
                    + "/data/output/" + SIGNATURES_FILENAME + " /data/output ";

            String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams, null);
            logger.info("Docker command line: " + cmdline);

            return cmdline;
        }
    }
