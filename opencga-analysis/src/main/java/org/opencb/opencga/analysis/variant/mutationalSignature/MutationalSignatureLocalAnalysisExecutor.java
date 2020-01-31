package org.opencb.opencga.analysis.variant.mutationalSignature;

import org.opencb.biodata.models.core.GenomeSequenceFeature;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.GenomicRegionClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.MutationalSignatureAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.IOException;
import java.util.*;

@ToolExecutor(id="opencga-local", tool = MutationalSignatureAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class MutationalSignatureLocalAnalysisExecutor extends MutationalSignatureAnalysisExecutor implements VariantStorageToolExecutor {

    private GenomicRegionClient regionClient;
    private static int BATCH_SIZE = 200;

    @Override
    public void run() throws ToolException {
        Map<String, Map<String, Double>> freqMap = initFreqMap();
        try {
            VariantStorageManager storageManager = getVariantStorageManager();

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
                if (freqMap.containsKey(key)) {
                    String region = variant.getChromosome() + ":" + (variant.getStart() - 1) + "-" + (variant.getEnd() + 1);
                    regionAlleleMap.put(region, key);
                    if (regionAlleleMap.size() >= BATCH_SIZE) {
                        updateCounterMap(regionAlleleMap, freqMap);
                        regionAlleleMap.clear();


                        // For testing
                        break;
                    }
                }
            }
            if (regionAlleleMap.size() > 0) {
                updateCounterMap(regionAlleleMap, freqMap);
            }
        } catch (Exception e) {
            throw new ToolExecutorException(e);
        }

        normalizeFreqMap(freqMap);
        writeResult(freqMap);
    }

    private void updateCounterMap(Map<String, String> regionAlleleMap, Map<String, Map<String, Double>> freqMap) throws IOException {
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
                    if (freqMap.get(key).containsKey(sequence)) {
                        freqMap.get(key).put(sequence, freqMap.get(key).get(sequence) + 1);
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
