package org.opencb.opencga.analysis.variant.mutationalSignature;

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
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor implements VariantStorageToolExecutor {

    private GenomicRegionClient regionClient;
    private static int BATCH_SIZE = 200;

    @Override
    public void run() throws ToolException {
        Map<String, Map<String, Double>> countMap = initFreqMap();
        try {
            VariantStorageManager storageManager = getVariantStorageManager();

            // Compute signature profile: contextual frequencies of each type of base substitution

            // TODO fix it using cellbase utils from storage manager
//            regionClient = storageManager.getCellBaseUtils(getStudy(), getToken()).getCellBaseClient().getGenomicRegionClient();
            String assembly = "grch37";
            String species = "hsapiens";

            CellBaseClient cellBaseClient = new CellBaseClient(species, assembly, new ClientConfiguration().setVersion("v4")
                    .setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 10)));

            regionClient = cellBaseClient.getGenomicRegionClient();

            Query query = new Query()
                    .append(VariantQueryParam.STUDY.key(), getStudy())
                    .append(VariantQueryParam.SAMPLE.key(), getSampleName())
                    .append(VariantQueryParam.TYPE.key(), "SNV");

            Map<String, String> regionAlleleMap = new HashMap<>();
            VariantDBIterator iterator = storageManager.iterator(query, new QueryOptions(), getToken());
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


                        // For testing
                        break;
                    }
                }
            }
            if (regionAlleleMap.size() > 0) {
                updateCounterMap(regionAlleleMap, countMap);
            }

            // Write context
            writeCountMap(countMap, getOutDir().resolve("context.txt").toFile());

            // To compare, download signatures probabilities at
            String link = "http://bioinfo.hpc.cam.ac.uk/opencb/opencga/analysis/cancer-signature/signatures_probabilities_v2.txt";
            InputStream in = new URL(link).openStream();
            Files.copy(in, getOutDir().resolve(Paths.get(new File(link).getName())), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            throw new ToolExecutorException(e);
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
}
