package org.opencb.opencga.storage.core.variant.analysis;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 01/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSampleFilter {

    private final VariantDBAdaptor dbAdaptor;

    public VariantSampleFilter(VariantDBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
    }

    public Map<String, List<Variant>> getSamplesInAnyVariants(List<?> variants, String study, List<String> genotypes) {
        List<String> variantsList = variants.stream().map(Object::toString).collect(Collectors.toList());
        Query query = new Query(VariantDBAdaptor.VariantQueryParams.ID.key(), variantsList)
                .append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study)
                .append(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), study);
        return getSamplesInAnyVariants(query, genotypes);
    }

    public Map<String, List<Variant>> getSamplesInAnyVariants(Query query, List<String> genotypes) {
        Set<String> samples = getSamplesList(query);

        Map<String, List<Variant>> map = new HashMap<>(samples.size());
        for (String sample : samples) {
            map.put(sample, new ArrayList<>());
        }

        Set<String> genotypesSet = new HashSet<>(genotypes);
        iterate(query, (variant, sample, gt) -> {
            if (genotypesSet.contains(gt)) {
                map.get(sample).add(variant);
            }
        });

        for (String sample : samples) {
            if (map.get(sample).isEmpty()) {
                map.remove(sample);
            }
        }

        return map;
    }

    public Collection<String> getSamplesInAllVariants(List<?> variants, String study, List<String> genotypes) {
        List<String> variantsList = variants.stream().map(Object::toString).collect(Collectors.toList());
        Query query = new Query(VariantDBAdaptor.VariantQueryParams.ID.key(), variantsList)
                .append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study)
                .append(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), study);
        return getSamplesInAllVariants(query, genotypes);
    }

    public Collection<String> getSamplesInAllVariants(Query query, List<String> genotypes) {

        Set<String> samples = getSamplesList(query);

        Set<String> genotypesSet = new HashSet<>(genotypes);
        iterate(query, (variant, sample, gt) -> {
            if (!genotypesSet.contains(gt)) {
                if (samples.remove(sample)) {
                    System.out.println("Variant " + variant + " sample " + sample + " gt: " + gt);
                }
            }
        });

        return samples;
    }

    @FunctionalInterface
    interface GenotypeWalker {
        void accept(Variant variant, String sample, String gt);
    }

    public void iterate(Query query, GenotypeWalker walker) {
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(VariantField.STUDIES_SAMPLES_DATA))
                .append(QueryOptions.LIMIT, 100);
        dbAdaptor.forEach(query, variant -> {
            StudyEntry studyEntry = variant.getStudies().get(0);
            Integer gtIdx = studyEntry.getFormatPositions().get("GT");
            if (gtIdx == null || gtIdx < 0) {
                throw new VariantQueryException("");
            }

            int sampleIdx = 0;
            for (String sample : studyEntry.getOrderedSamplesName()) {
                String gt = studyEntry.getSamplesData().get(sampleIdx).get(gtIdx);
                walker.accept(variant, sample, gt);
                sampleIdx++;
            }
        }, options);
    }

    public Set<String> getSamplesList(Query query) {
        Map<String, List<String>> studies = dbAdaptor.getDBAdaptorUtils().getSamplesMetadata(query);
        if (studies.size() != 1) {
            throw new VariantQueryException("Unable to process with " + studies.size() + " studies.");
        }
        Set<String> samples = new HashSet<>(studies.entrySet().iterator().next().getValue());
        if (samples.isEmpty()) {
            throw new VariantQueryException("Unable to get samples!");
        }
        return samples;
    }
}
