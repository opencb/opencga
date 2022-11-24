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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.Signature;
import org.opencb.biodata.models.clinical.qc.SignatureFitting;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.BreakendMate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.MutationalSignatureAnalysisParams;
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

import static org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis.*;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor
        implements StorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-ext-tools:"
            + GitRepositoryState.get().getBuildVersion();

    private Path opencgaHome;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, CatalogException, IOException, StorageEngineException {
        opencgaHome = Paths.get(getExecutorParams().getString("opencgaHome"));

        // Check genome context file for that sample, and create it if necessary
        if (StringUtils.isNotEmpty(getSkip())
                && getSkip().contains(MutationalSignatureAnalysisParams.SIGNATURE_CATALOGUE_SKIP_VALUE)
                && getSkip().contains(MutationalSignatureAnalysisParams.SIGNATURE_FITTING_SKIP_VALUE)) {
            // Only compute genome context file
            // TODO: overwrite support !
            File indexFile = checkGenomeContextFile();
            logger.info("Checking genome context file {} for sample {}", indexFile.getAbsolutePath(), getSample());
        }

        if (StringUtils.isEmpty(getSkip()) || (!getSkip().contains(MutationalSignatureAnalysisParams.SIGNATURE_CATALOGUE_SKIP_VALUE))) {
            // Get first variant to check where the genome context is stored
            Query query = new Query();
            if (getQuery() != null) {
                query.putAll(getQuery());
            }
            // Overwrite study and type (SNV)
            String type = query.getString(VariantQueryParam.TYPE.key());
            if (type.equals(VariantType.SNV.name())) {
                // SNV
                logger.info("Computing catalogue (mutational signature) for SNV variants");

                // TODO: overwrite support !
                File indexFile = checkGenomeContextFile();
                logger.info("Mutational signature analysis is using the genome context file {} for sample {}", indexFile.getAbsolutePath(),
                        getSample());

                query.append(VariantQueryParam.STUDY.key(), getStudy()).append(VariantQueryParam.TYPE.key(), VariantType.SNV);

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

                // Run mutational analysis taking into account that the genome context is stored in an index file,
                // if the genome context file does not exist, it will be created !!!
                computeSignatureCatalogueSNV(indexFile);
            } else {
                // SV
                logger.info("Computing catalogue (mutational signature) for SV variants");
                computeSignatureCatalogueSV();
            }
        }

        if (StringUtils.isEmpty(getSkip()) || (!getSkip().contains(MutationalSignatureAnalysisParams.SIGNATURE_FITTING_SKIP_VALUE))) {
            // Run R script for fitting signature
            computeSignatureFitting();
        }
    }

    private File checkGenomeContextFile() throws ToolExecutorException {
        // Context index filename
        File indexFile;
        try {
            indexFile = MutationalSignatureAnalysis.getGenomeContextFile(getSample(), getStudy(), getVariantStorageManager().getCatalogManager(), getToken());
        } catch (CatalogException | ToolException e) {
            indexFile = null;
        }
        if (indexFile != null && indexFile.exists()) {
            return indexFile;
        }

        // The genome context file does not exist, we have to create it !!!
        indexFile = getOutDir().resolve(MutationalSignatureAnalysis.getContextIndexFilename(getSample(), getAssembly())).toFile();
        createGenomeContextFile(indexFile);

        if (!indexFile.exists()) {
            throw new ToolExecutorException("Could not create the genome context index file for sample " + getSample());
        }
        return indexFile;
    }

    private void createGenomeContextFile(File indexFile) throws ToolExecutorException {
        try {
            // First,
            ResourceUtils.DownloadedRefGenome refGenome = ResourceUtils.downloadRefGenome(getAssembly(), getOutDir(),
                    opencgaHome);

            if (refGenome == null) {
                throw new ToolExecutorException("Something wrong happened accessing reference genome, check local path"
                        + " and public repository");
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

            try (PrintWriter pw = new PrintWriter(indexFile);
                 BlockCompressedIndexedFastaSequenceFile indexed = new BlockCompressedIndexedFastaSequenceFile(
                         refGenomePath, new FastaSequenceIndex(new File(base + ".fai")),
                         GZIIndex.loadIndex(Paths.get(base + ".gzi")))) {
                while (iterator.hasNext()) {
                    Variant variant = iterator.next();

                    try {
                        // Accessing to the context sequence and write it into the context index file
                        ReferenceSequence refSeq = indexed.getSubsequenceAt(variant.getChromosome(), variant.getStart() - 1,
                                variant.getEnd() + 1);
                        String sequence = new String(refSeq.getBases());

                        // Write context index
                        pw.println(variant.toString() + "\t" + sequence);
                    } catch (Exception e) {
                        logger.warn("When creating genome context file for mutational signature analysis, ignoring variant "
                                + variant.toStringSimple() + ". " + e.getMessage());
                    }
                }
            }

        } catch (IOException | CatalogException | ToolException | StorageEngineException e) {
            throw new ToolExecutorException(e);
        }
    }

    private void updateCountMap(Variant variant, String sequence, Map<String, Map<String, Integer>> countMap) {
        try {
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
            }
        } catch (Exception e) {
            logger.warn("When counting mutational signature substitutions, ignoring variant " + variant.toStringSimple()
                    + " with sequence " + sequence + ". " + e.getMessage());
        }
    }

    public void computeSignatureCatalogueSNV(File indexFile) throws ToolExecutorException {
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
            // Ovewrite study and type (SNV)
            query.append(VariantQueryParam.STUDY.key(), getStudy()).append(VariantQueryParam.TYPE.key(), VariantType.SNV);

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id");

            VariantDBIterator iterator = getVariantStorageManager().iterator(query, queryOptions, getToken());

            Map<String, Map<String, Integer>> countMap = initCountMap();

            while (iterator.hasNext()) {
                Variant variant = iterator.next();

                // Update count map
                updateCountMap(variant, indexMap.get(variant.toString()), countMap);
            }

            // Write context counts
            File cataloguesFile = getOutDir().resolve(CATALOGUES_FILENAME_DEFAULT).toFile();
            writeCountMap(getSample(), countMap, cataloguesFile);

            // Check catalogue file before parsing and creating the mutational signature data model
            if (!cataloguesFile.exists()) {
                throw new ToolExecutorException("Something wrong happened: counts file " + CATALOGUES_FILENAME_DEFAULT + " could not be"
                        + " generated");
            }
            List<Signature.GenomeContextCount> genomeContextCounts = parseCatalogueResults(getOutDir());
            Signature signature = new Signature()
                    .setId(getQueryId())
                    .setDescription(getQueryDescription())
                    .setQuery(query)
                    .setType("SNV")
                    .setCounts(genomeContextCounts);

            JacksonUtils.getDefaultObjectMapper().writerFor(Signature.class).writeValue(getOutDir()
                    .resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_DATA_MODEL_FILENAME).toFile(), signature);
        } catch (IOException | CatalogException | StorageEngineException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    public void computeSignatureCatalogueSV() throws ToolExecutorException {
        try {
            // Get variant iterator
            Query query = new Query();
            if (getQuery() != null) {
                query.putAll(getQuery());
            }
            // Overwrite study and types related to SV
            query.put(VariantQueryParam.STUDY.key(), getStudy());
            query.put(VariantQueryParam.TYPE.key(), VariantType.DELETION + "," + VariantType.BREAKEND + "," + VariantType.DUPLICATION  + ","
                    + VariantType.TANDEM_DUPLICATION + "," + VariantType.INVERSION + "," + VariantType.TRANSLOCATION);

            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,sv,studies");

            logger.info("Query: {}", query.toJson());
            logger.info("Query options: {}", queryOptions.toJson());
            VariantDBIterator iterator = getVariantStorageManager().iterator(query, queryOptions, getToken());

            Map<String, Integer> countMap = new HashMap<>();

            while (iterator.hasNext()) {
                Variant variant = iterator.next();

                // Update count map
                String clusteredKey = getClusteredKey(variant);
                String typeKey = getTypeKey(variant);
                if (typeKey == null) {
//                    logger.info("Skipping variant {}: SV type not supported {}", variant.toStringSimple(), variant.getType());
                    continue;
                }
                String lengthKey = getLengthKey(variant);
                if (lengthKey == null) {
//                    logger.info("Skipping variant {}: it could not compute the distance to the variant mate", variant.toStringSimple());
                    continue;
                }
                String key = clusteredKey + "_" + typeKey;
                if (!lengthKey.equals(LENGTH_NA)) {
                    key += ("_" + lengthKey);
                }
                if (countMap.containsKey(key)) {
                    countMap.put(key, 1 + countMap.get(key));
                } else {
                    countMap.put(key, 1);
                }
            }

            logger.info("Count map size = {}", countMap.size());
            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                logger.info("context = {}, count = {}", entry.getKey(), entry.getValue());
            }

            List<Signature.GenomeContextCount> genomeContextCounts = new LinkedList<>();
            for (String clustered: new LinkedList<>(Arrays.asList(CLUSTERED, NON_CLUSTERED))) {
                for (String type: new LinkedList<>(Arrays.asList(TYPE_DEL, TYPE_TDS, TYPE_INV))) {
                    for (String length : new LinkedList<>(Arrays.asList(LENGTH_1_10Kb, LENGTH_10Kb_100Kb, LENGTH_100Kb_1Mb, LENGTH_1Mb_10Mb,
                            LENGTH_10Mb))) {
                        String key = clustered + "_" + type + "_" + length;
                        genomeContextCounts.add(new Signature.GenomeContextCount(key, countMap.containsKey(key) ? countMap.get(key) : 0));
                    }
                }
                String key = clustered + "_" + TYPE_TRANS;
                genomeContextCounts.add(new Signature.GenomeContextCount(key, countMap.containsKey(key) ? countMap.get(key) : 0));
            }

            // Write catalogue file
            PrintWriter pw = new PrintWriter(getOutDir().resolve(CATALOGUES_FILENAME_DEFAULT).toFile());
            pw.write(query.getString(VariantQueryParam.SAMPLE.key()));
            pw.write("\n");
            for (Signature.GenomeContextCount counts : genomeContextCounts) {
                pw.write(counts.getContext() + "\t" + counts.getTotal() + "\n");
            }
            pw.close();

            Signature signature = new Signature()
                    .setId(getQueryId())
                    .setDescription(getQueryDescription())
                    .setQuery(query)
                    .setType("SV")
                    .setCounts(genomeContextCounts);

            JacksonUtils.getDefaultObjectMapper().writerFor(Signature.class).writeValue(getOutDir()
                    .resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_DATA_MODEL_FILENAME).toFile(), signature);
        } catch (IOException | CatalogException | StorageEngineException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    private String getClusteredKey(Variant variant) {
        return NON_CLUSTERED;
    }

    private String getTypeKey(Variant variant) {
        String variantType = variant.getType() != null ? variant.getType().name() : "";
        if (CollectionUtils.isNotEmpty(variant.getStudies()) && CollectionUtils.isNotEmpty(variant.getStudies().get(0).getFiles())) {
            for (FileEntry file : variant.getStudies().get(0).getFiles()) {
                if (file.getData() != null && file.getData().containsKey("EXT_SVTYPE")) {
                    variantType = file.getData().get("EXT_SVTYPE");
                    break;
                }
            }
        }

        switch (variantType) {
            case "DELETION":
                return TYPE_DEL;
            case "DUPLICATION":
            case "TANDEM_DUPLICATION":
                return TYPE_TDS;
            case "INVERSION":
                return TYPE_INV;
            case "TRANSLOCATION":
                return TYPE_TRANS;
        }
        return null;
    }

    private String getLengthKey(Variant variant) {
        if (variant.getSv() == null || variant.getSv().getBreakend() == null || variant.getSv().getBreakend().getMate() == null) {
            return null;
        }
        BreakendMate mate = variant.getSv().getBreakend().getMate();
        if (variant.getChromosome().equals(mate.getChromosome())) {
            int length = Math.abs(mate.getPosition() - variant.getStart());
            if (length <= 10000) {
                return LENGTH_1_10Kb;
            } else if (length <= 100000) {
                return LENGTH_10Kb_100Kb;
            } else if (length <= 1000000) {
                return LENGTH_100Kb_1Mb;
            } else if (length <= 10000000) {
                return LENGTH_1Mb_10Mb;
            }
            return LENGTH_10Mb;
        } else {
            if (variant.getType() == VariantType.TRANSLOCATION) {
                return LENGTH_NA;
            }
        }
        return null;
    }

    private void computeSignatureFitting() throws IOException, ToolException, CatalogException {
        File cataloguesFile = getOutDir().resolve(CATALOGUES_FILENAME_DEFAULT).toFile();
        if (!cataloguesFile.exists()) {
            // Get counts from sample
            CatalogManager catalogManager = getVariantStorageManager().getCatalogManager();
            // Check sample
            String study = catalogManager.getStudyManager().get(getStudy(), QueryOptions.empty(), getToken()).first().getFqn();
            OpenCGAResult<Sample> sampleResult = catalogManager.getSampleManager().get(study, getSample(), QueryOptions.empty(),
                    getToken());
            if (sampleResult.getNumResults() != 1) {
                throw new ToolException("Unable to compute mutational signature analysis. Sample '" + getSample() + "' not found");
            }
            Sample sample = sampleResult.first();
            logger.info("Searching catalogue counts from quality control for sample " + getSample());
            if (sample.getQualityControl() != null && sample.getQualityControl().getVariant() != null
                    && CollectionUtils.isNotEmpty(sample.getQualityControl().getVariant().getSignatures())) {
                logger.info("Searching in " + sample.getQualityControl().getVariant().getSignatures().size() + " signatues");
                for (Signature signature : sample.getQualityControl().getVariant().getSignatures()) {
                    logger.info("Matching ? " + getQueryId() + " vs " + signature.getId());
                    if (getQueryId().equals(signature.getId())) {
                        // Write catalogue file
                        try (PrintWriter pw = new PrintWriter(cataloguesFile)) {
                            pw.println(getSample());
                            for (Signature.GenomeContextCount count : signature.getCounts()) {
                                pw.println(count.getContext() + "\t" + count.getTotal());
                            }
                            pw.close();
                        } catch (Exception e) {
                            throw new ToolException("Error writing catalogue output file: " + cataloguesFile.getName(), e);
                        }
                        logger.info("Found catalogue {} and written in {}", signature.getId(), cataloguesFile.getAbsolutePath());
                        break;
                    }
                }
            }
            if (!cataloguesFile.exists()) {
                throw new ToolException("Could not find mutational signagure catalogue (counts) file: " + cataloguesFile.getName());
            }
        }

        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(), "/data/input"));
        if (StringUtils.isNotEmpty(getSignaturesFile())) {
            File signaturesFile = new File(getSignaturesFile());
            if (signaturesFile.exists()) {
                inputBindings.add(new AbstractMap.SimpleEntry<>(signaturesFile.getParent(), "/data/signaturesFile"));
            }
        }
        if (StringUtils.isNotEmpty(getRareSignaturesFile())) {
            File rareSignaturesFile = new File(getRareSignaturesFile());
            if (rareSignaturesFile.exists()) {
                inputBindings.add(new AbstractMap.SimpleEntry<>(rareSignaturesFile.getParent(), "/data/rareSignaturesFile"));
            }
        }
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir()
                .toAbsolutePath().toString(), "/data/output");
        StringBuilder scriptParams = new StringBuilder("R CMD Rscript --vanilla ")
                .append("/opt/opencga/signature.tools.lib/scripts/signatureFit")
                .append(" --catalogues=/data/input/").append(cataloguesFile.getName())
                .append(" --outdir=/data/output");
        if (StringUtils.isNotEmpty(getFitMethod())) {
            scriptParams.append(" --fitmethod=").append(getFitMethod());
        }
        if (StringUtils.isNotEmpty(getSigVersion())) {
            scriptParams.append(" --sigversion=").append(getSigVersion());
        }
        if (StringUtils.isNotEmpty(getOrgan())) {
            scriptParams.append(" --organ=").append(getOrgan());
        }
        if (getThresholdPerc() != null) {
            scriptParams.append(" --thresholdperc=").append(getThresholdPerc());
        }
        if (getThresholdPval() != null) {
            scriptParams.append(" --thresholdpval=").append(getThresholdPval());
        }
        if (getMaxRareSigs() != null) {
            scriptParams.append(" --maxraresigs=").append(getMaxRareSigs());
        }
        if (getnBoot() != null) {
            scriptParams.append(" -b --nboot=").append(getnBoot());
        }
        if (StringUtils.isNotEmpty(getSignaturesFile()) && new File(getSignaturesFile()).exists()) {
            scriptParams.append(" --signaturesfile=/data/signaturesFile/").append(new File(getSignaturesFile()).getName());
        }
        if (StringUtils.isNotEmpty(getRareSignaturesFile()) && new File(getRareSignaturesFile()).exists()) {
            scriptParams.append(" --raresignaturesfile=/data/rareSignaturesFile/").append(new File(getRareSignaturesFile()).getName());
        }
        switch (getAssembly()) {
            case "GRCh37": {
                scriptParams.append(" --genomev=hg19");
                break;
            }
            case "GRCh38": {
                scriptParams.append(" --genomev=hg38");
                break;
            }
        }

        String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams.toString(),
                null);
        logger.info("Docker command line: " + cmdline);

        // Check fitting file before parsing and creating the mutational signature fitting data model
        File signatureCoeffsFile = getOutDir().resolve(SIGNATURE_COEFFS_FILENAME).toFile();
        if (!signatureCoeffsFile.exists()) {
            throw new ToolExecutorException("Something wrong happened: signature coeffs. file " + SIGNATURE_COEFFS_FILENAME + " could not"
                    + " be generated");
        }
        SignatureFitting signatureFitting = parseFittingResults(getOutDir(), getFitId(), getFitMethod(), getSigVersion(), getnBoot(),
                getOrgan(), getThresholdPerc(), getThresholdPval(), getMaxRareSigs());
        JacksonUtils.getDefaultObjectMapper().writerFor(SignatureFitting.class).writeValue(getOutDir()
                .resolve(MutationalSignatureAnalysis.MUTATIONAL_SIGNATURE_FITTING_DATA_MODEL_FILENAME).toFile(), signatureFitting);
    }
}
