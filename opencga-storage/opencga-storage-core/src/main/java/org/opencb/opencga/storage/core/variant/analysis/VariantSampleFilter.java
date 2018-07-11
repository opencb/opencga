/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.variant.analysis;

import com.google.common.base.Throwables;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created on 01/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSampleFilter {

    private final VariantIterable iterable;
    private int maxVariants;
    protected final Logger logger = LoggerFactory.getLogger(VariantSampleFilter.class);

    public VariantSampleFilter(VariantIterable iterable) {
        this.iterable = iterable;
        maxVariants = 50;
    }

    public Map<String, Set<Variant>> getSamplesInAnyVariants(List<?> variants, String study, List<String> samples, List<String> genotypes) {
        List<String> variantsList = variants.stream().map(Object::toString).collect(Collectors.toList());
        Query query = new Query(VariantQueryParam.ID.key(), variantsList)
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), samples);
        return getSamplesInAnyVariants(query, genotypes);
    }

    public Map<String, Set<Variant>> getSamplesInAnyVariants(Query query, List<String> genotypes) {

        Map<String, Set<Variant>> map = new HashMap<>();
        Set<String> samples = new HashSet<>();

        Set<String> genotypesSet = new HashSet<>(genotypes);
        List<GenotypeClass> genotypeClasses = getGenotypeClasses(genotypesSet);

        iterate(query, variant -> {
            getSamplesSet(variant, samples);
            for (String sample : samples) {
                map.put(sample, new HashSet<>());
            }
        }, (variant, sample, gt) -> {
            if (isValidGenotype(genotypesSet, genotypeClasses, gt)) {
                map.get(sample).add(variant);
            }
            return true;
        });

        for (String sample : samples) {
            if (map.get(sample).isEmpty()) {
                map.remove(sample);
            }
        }

        return map;
    }

    public Collection<String> getSamplesInAllVariants(List<?> variants, String study, List<String> samples, List<String> genotypes) {
        List<String> variantsList = variants.stream().map(Object::toString).collect(Collectors.toList());
        Query query = new Query(VariantQueryParam.ID.key(), variantsList)
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.INCLUDE_STUDY.key(), study)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), samples);
        return getSamplesInAllVariants(query, genotypes);
    }

    public Collection<String> getSamplesInAllVariants(Query query, List<String> genotypes) {
        Set<String> samples = new HashSet<>();
        Set<String> genotypesSet = new HashSet<>(genotypes);
        List<GenotypeClass> genotypeClasses = getGenotypeClasses(genotypesSet);
        iterate(query, variant -> getSamplesSet(variant, samples),
                (variant, sample, gt) -> {
                    // Remove if not a valid genotype
                    if (!isValidGenotype(genotypesSet, genotypeClasses, gt)) {
                        if (samples.remove(sample)) {
                            logger.debug("variant: {}, sample: {}, gt: {}", variant, sample, gt);
                            if (sample.isEmpty()) {
                                return false;
                            }
                        }
                    }
                    return true;
                });
        return samples;
    }

    @FunctionalInterface
    interface GenotypeWalker {
        boolean accept(Variant variant, String sample, String gt);
    }

    protected void iterate(Query query, Consumer<Variant> init, GenotypeWalker walker) {
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Collections.singletonList(VariantField.STUDIES_SAMPLES_DATA))
                .append(QueryOptions.LIMIT, maxVariants + 1);
        try (VariantDBIterator iterator = iterable.iterator(query, options)) {
            if (!iterator.hasNext()) {
                return;
            }
            int numVariants = 0;
            while (iterator.hasNext()) {
                Variant variant = iterator.next();
                if (numVariants == 0) {
                    init.accept(variant);
                } else if (numVariants == maxVariants) {
                    throw new VariantQueryException("Error! Limit reached with more than " + maxVariants + " variants!");
                }
                numVariants++;
                StudyEntry studyEntry = variant.getStudies().get(0);
                Integer gtIdx = studyEntry.getFormatPositions().get("GT");
                if (gtIdx == null || gtIdx < 0) {
                    throw new VariantQueryException("Missing GT at variant " + variant);
                }

                int sampleIdx = 0;
                for (String sample : studyEntry.getOrderedSamplesName()) {
                    String gt = studyEntry.getSamplesData().get(sampleIdx).get(gtIdx);
                    if (!walker.accept(variant, sample, gt)) {
                        break;
                    }
                    sampleIdx++;
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private Set<String> getSamplesSet(Variant variant, Set<String> samples) {
        if (variant.getStudies().size() != 1) {
            throw new VariantQueryException("Unable to process with " + variant.getStudies().size() + " studies.");
        }
        samples.addAll(variant.getStudies().get(0).getSamplesName());
        if (samples.isEmpty()) {
            throw new VariantQueryException("Unable to get samples!");
        }
        return samples;
    }

    private boolean isValidGenotype(Set<String> genotypesSet, List<GenotypeClass> genotypeClasses, String gt) {
        return genotypesSet.contains(gt) || !genotypeClasses.isEmpty() && genotypeClasses.stream().anyMatch(gc -> gc.test(gt));
    }

    private List<GenotypeClass> getGenotypeClasses(Set<String> genotypesSet) {
        List<GenotypeClass> genotypeClasses = new ArrayList<>();
        Iterator<String> iterator = genotypesSet.iterator();
        while (iterator.hasNext()) {
            String genotype = iterator.next();
            GenotypeClass genotypeClass = GenotypeClass.from(genotype);
            if (genotypeClass != null) {
                genotypeClasses.add(genotypeClass);
                iterator.remove();
            }
        }
        return genotypeClasses;
    }
}
