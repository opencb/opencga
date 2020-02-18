package org.opencb.opencga.analysis.variant.mutationalSignature;

import htsjdk.samtools.reference.BlockCompressedIndexedFastaSequenceFile;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.GZIIndex;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor implements VariantStorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-r:2.0.0-dev";

    public final static String CONTEXT_FILENAME = "context.txt";
    public final static String SIGNATURES_FILENAME = "signatures_probabilities_v2.txt";

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
            ResourceUtils.DownloadedRefGenome refGenome = ResourceUtils.downloadRefGenome(ResourceUtils.Species.hsapiens,
                    ResourceUtils.Assembly.GRCh38, ResourceUtils.Authority.Ensembl, getOutDir());

            if (refGenome == null) {
                throw new ToolExecutorException("Something wrong happened downloading reference genome from " + ResourceUtils.URL);
            }
            // Read mutation context from reference genome (.gz, .gz.fai and .gz.gzi files)
            BlockCompressedIndexedFastaSequenceFile indexed = new BlockCompressedIndexedFastaSequenceFile(refGenome.getGzFile().toPath(),
                    new FastaSequenceIndex(refGenome.getFaiFile()), GZIIndex.loadIndex(refGenome.getGziFile().toPath()));

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

            // Write context
            writeCountMap(countMap, getOutDir().resolve(CONTEXT_FILENAME).toFile());

            // To compare, downloadAnalysis signatures probabilities at
            File signatureFile = ResourceUtils.downloadAnalysis(MutationalSignatureAnalysis.ID, SIGNATURES_FILENAME, getOutDir());
            if (signatureFile == null) {
                throw new ToolExecutorException("Error downloading mutational signatures file from " + ResourceUtils.URL);
            }

            // Execute R script in docker
            String rScriptPath = getExecutorParams().getString("opencgaHome") + "/analysis/R/" + getToolId();
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            inputBindings.add(new AbstractMap.SimpleEntry<>(rScriptPath, "/data/input"));
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    "/data/output");
            String scriptParams = "R CMD Rscript --vanilla /data/input/mutational-signature.r /data/output/" + CONTEXT_FILENAME + " "
                    + "/data/output/" + SIGNATURES_FILENAME + " /data/output";
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
