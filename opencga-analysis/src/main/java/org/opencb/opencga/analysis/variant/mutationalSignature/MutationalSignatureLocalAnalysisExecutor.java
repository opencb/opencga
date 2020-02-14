package org.opencb.opencga.analysis.variant.mutationalSignature;

import htsjdk.samtools.reference.BlockCompressedIndexedFastaSequenceFile;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.GZIIndex;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import org.opencb.commons.utils.DockerUtils;
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
import java.util.*;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor implements VariantStorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencga-r";


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
                    ResourceUtils.Assembly.Grch38, ResourceUtils.Authority.Ensembl, getOutDir());

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
            writeCountMap(countMap, getOutDir().resolve("context.txt").toFile());

            // To compare, download signatures probabilities at
            File signatureFile = ResourceUtils.download(MutationalSignatureAnalysis.ID, "signatures_probabilities_v2.txt", getOutDir());
            if (signatureFile == null) {
                throw new ToolExecutorException("Error downloading mutational signatures file from " + ResourceUtils.URL);
            }
        } catch (Exception e) {
            throw new ToolExecutorException(e);
        }

        // Execute R script using docker
        String rScriptPath = executorParams.getString("opencgaHome") + "/analysis/R/" + getToolId();
        List<Pair<String, String>> bindings = new ArrayList<>();
        bindings.add(new ImmutablePair<>(rScriptPath, "/data/input"));
        bindings.add(new ImmutablePair<>(getOutDir().toAbsolutePath().toString(), "/data/output"));
        String params = "R CMD Rscript --vanilla /data/input/mutational-signature.r /data/output/context.txt "
                + "/data/output/signatures_probabilities_v2.txt /data/output";
        DockerUtils.run(R_DOCKER_IMAGE, bindings, params);

        // Check output files
        if (!new File(getOutDir() + "/signature_summary.png").exists()
                || !new File(getOutDir() + "/signature_coefficients.json").exists()) {
            String msg = "Something wrong executing mutational signature.";
            throw new ToolException(msg);
        }
    }
}
