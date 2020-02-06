package org.opencb.opencga.analysis.variant.mutationalSignature;

import htsjdk.samtools.reference.BlockCompressedIndexedFastaSequenceFile;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.GZIIndex;
import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.opencb.biodata.models.core.GenomeSequenceFeature;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.GenomicRegionClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.analysis.wrappers.OpenCgaWrapperAnalysis;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.*;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor implements VariantStorageToolExecutor {

    private GenomicRegionClient regionClient;

    private File fastaFile;
    private File faiFile;
    private File gziFile;

    private final static int BATCH_SIZE = 200;

    public final static String R_DOCKER_IMAGE = "opencga-r";

    public final static String RESOURCES_URL = "http://bioinfo.hpc.cam.ac.uk/opencb/";
    public final static String SIGNATURES_URL = RESOURCES_URL + "opencga/analysis/mutational-signature/signatures_probabilities_v2.txt";
    public final static String FASTA_URL = RESOURCES_URL + "commons/reference-genomes/ensembl/Homo_sapiens.GRCh38.dna.toplevel.fa.gz";
    public final static String FAI_URL = RESOURCES_URL + "commons/reference-genomes/ensembl/Homo_sapiens.GRCh38.dna.toplevel.fa.gz.fai";
    public final static String GZI_URL = RESOURCES_URL + "commons/reference-genomes/ensembl/Homo_sapiens.GRCh38.dna.toplevel.fa.gz.gzi";

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

            VariantDBIterator iterator = storageManager.iterator(query, new QueryOptions(), getToken());

            // FIXME to make URLs dependent on assembly (and Ensembl/NCBI ?)
            fastaFile = ResourceUtils.download(new URL(FASTA_URL), getOutDir());
            faiFile = ResourceUtils.download(new URL(FAI_URL), getOutDir());
            gziFile = ResourceUtils.download(new URL(GZI_URL), getOutDir());

            if (fastaFile != null && faiFile != null && gziFile != null) {
                updateCountMapFromFasta(iterator, countMap);
            } else {
                updateCountMapFromCellBase(iterator, countMap);
            }

            // Write context
            writeCountMap(countMap, getOutDir().resolve("context.txt").toFile());

            // To compare, download signatures probabilities at
            File signatureFile = ResourceUtils.download(new URL(SIGNATURES_URL), getOutDir());
            if (signatureFile == null) {
                throw new ToolExecutorException("Error downloading mutational signatures file: " + SIGNATURES_URL);
            }
        } catch (Exception e) {
            throw new ToolExecutorException(e);
        }

        // Execute R script
        String commandLine = getCommandLine();
        System.out.println("Mutational signature command line:\n" + commandLine);
        try {
            // Execute command
            Command cmd = new Command(commandLine);
            cmd.run();

            // Check errors by reading the stdout and stderr files
            if (!new File(getOutDir() + "/signature_summary.png").exists()
                    || !new File(getOutDir() + "/signature_coefficients.json").exists()) {
                String msg = "Something wrong executing mutational signature.";
                throw new ToolException(msg);
            }
        } catch (Exception e) {
            throw new ToolException(e);
        }
    }

    private void updateCountMapFromFasta(VariantDBIterator iterator, Map<String, Map<String, Double>> countMap) throws IOException {
        BlockCompressedIndexedFastaSequenceFile indexed = new BlockCompressedIndexedFastaSequenceFile(fastaFile.toPath(),
                new FastaSequenceIndex(faiFile), GZIIndex.loadIndex(gziFile.toPath()));

        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            String key = variant.getReference() + ">" + variant.getAlternate();
            if (countMap.containsKey(key)) {
                try {
                    ReferenceSequence refSeq = indexed.getSubsequenceAt(variant.getChromosome(), variant.getStart() - 1, variant.getEnd() + 1);
                    String sequence = new String(refSeq.getBases());

                    if (countMap.get(key).containsKey(sequence)) {
                        countMap.get(key).put(sequence, countMap.get(key).get(sequence) + 1);
                    }
                } catch (Exception e) {
                    System.out.println("Error getting context sequence for variant " + variant.toStringSimple() + ": " + e.getMessage());
                }
            }
        }
    }

    private void updateCountMapFromCellBase(VariantDBIterator iterator, Map<String, Map<String, Double>> countMap) throws IOException {
        Map<String, String> regionAlleleMap = new HashMap<>();

        // FIXME: use cellbase utils from storage manager
//            regionClient = storageManager.getCellBaseUtils(getStudy(), getToken()).getCellBaseClient().getGenomicRegionClient();
        String assembly = "grch38";
        String species = "hsapiens";

        CellBaseClient cellBaseClient = new CellBaseClient(species, assembly, new ClientConfiguration().setVersion("v4")
                .setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 10)));

        regionClient = cellBaseClient.getGenomicRegionClient();
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            variant.toStringSimple();
            String key = variant.getReference() + ">" + variant.getAlternate();
            if (countMap.containsKey(key)) {
                String region = variant.getChromosome() + ":" + (variant.getStart() - 1) + "-" + (variant.getEnd() + 1);
                regionAlleleMap.put(region, key);
                if (regionAlleleMap.size() >= BATCH_SIZE) {
                    updateCounterMap(regionAlleleMap, countMap);
                    regionAlleleMap.clear();
                }
            }
        }
        if (regionAlleleMap.size() > 0) {
            updateCounterMap(regionAlleleMap, countMap);
        }
    }

    private void updateCounterMap(Map<String, String> regionAlleleMap, Map<String, Map<String, Double>> countMap) throws IOException {
        String key;
        QueryResponse<GenomeSequenceFeature> response = regionClient.getSequence(new ArrayList(regionAlleleMap.keySet()),
                QueryOptions.empty());
        List<QueryResult<GenomeSequenceFeature>> seqFeatures = response.getResponse();
        for (QueryResult<GenomeSequenceFeature> seqFeature : seqFeatures) {
            if (CollectionUtils.isNotEmpty(seqFeature.getResult())) {
                GenomeSequenceFeature feature = seqFeature.getResult().get(0);
                String sequence = feature.getSequence();
                if (sequence.length() == 3) {
                    // Remember that GenomeSequenceFeature ID is equal to region
                    key = regionAlleleMap.get(seqFeature.getId());
                    if (countMap.get(key).containsKey(sequence)) {
                        countMap.get(key).put(sequence, countMap.get(key).get(sequence) + 1);
                    } else {
                        System.err.println("Error, key not found " + sequence + " for " + key);
                    }
                } else {
                    System.err.println("Error query for " + feature.getSequenceName() + ":" + feature.getStart() + "-"
                            + feature.getEnd() + ", sequence " + sequence);
                }
            } else {
                System.err.println("Empty results for " + seqFeature.getId());
            }
        }
    }

    public String getCommandLine() throws ToolException {
        // docker run --mount type=bind,source="<path to R script>",target="/data/input"
        // --mount type=bind,source="<path to output dir>",target="/data/output"
        // opencga-r R CMD Rscript --vanilla
        // /data/input/mutational-signature.r /data/output/context.txt /data/output/signatures_probabilities_v2.txt /data/output

        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        String rScriptPath = executorParams.getString("opencgaHome") + "/analysis/R/" + getToolId();
        sb.append("--mount type=bind,source=\"").append(rScriptPath).append("\",target=\"")
                .append(OpenCgaWrapperAnalysis.DOCKER_INPUT_PATH).append("\" ").append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(OpenCgaWrapperAnalysis.DOCKER_OUTPUT_PATH).append("\"");

        // Docker image and version
        sb.append(" ").append(R_DOCKER_IMAGE);

        // R command and parameters
        sb.append(" R CMD Rscript --vanilla");
        sb.append(" /data/input/mutational-signature.r /data/output/context.txt /data/output/signatures_probabilities_v2.txt /data/output");

        return sb.toString();
    }
}
