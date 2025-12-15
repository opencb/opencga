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

package org.opencb.opencga.storage.core.variant.dummy;

import com.google.common.collect.Iterators;
import htsjdk.variant.vcf.VCFConstants;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.filters.VariantFilterBuilder;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.VARIANT_ID;

/**
 * Created on 28/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantDBAdaptor implements VariantDBAdaptor {

    private final String dbName;
    private boolean closed = false;
    private Logger logger = LoggerFactory.getLogger(DummyVariantDBAdaptor.class);
    private final VariantStorageMetadataManager metadataManager;
    private static final String QUIET = "quiet";

    public DummyVariantDBAdaptor(String dbName) {
        this.dbName = dbName;
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
    }

    @Override
    public VariantQueryResult<Variant> get(ParsedVariantQuery query) {

        List<Variant> variants = new ArrayList<>();
        iterator(query).forEachRemaining(variants::add);

        return new VariantQueryResult<>(0, variants.size(), variants.size(), Collections.emptyList(), variants,
                DummyVariantStorageEngine.STORAGE_ENGINE_ID, query);
    }

    @Override
    public VariantQueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize) {
        return null;
    }

    @Override
    public DataResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options) {
        VariantAnnotation annotation = new VariantAnnotation();
        if (VariantAnnotationManager.CURRENT.equals(name) || name == null) {
            annotation.setId(VariantAnnotationManager.CURRENT);
        } else {
            // Ensure saved annotation exists
            ProjectMetadata.VariantAnnotationMetadata saved = getMetadataManager().getProjectMetadata().getAnnotation().getSaved(name);
            annotation.setId(saved.getName());
        }
        return new DataResult<VariantAnnotation>().setResults(Arrays.asList(annotation));
    }

    @Override
    public DataResult<Long> count(ParsedVariantQuery query) {
        AtomicLong count = new AtomicLong();
        iterator(query).forEachRemaining(variant -> {
            count.incrementAndGet();
        });
        return new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList(count.get()), count.get());
    }

    @Override
    public DataResult distinct(Query query, String field) {
        return null;
    }

    @Override
    public VariantDBIterator iterator(ParsedVariantQuery variantQuery) {
        QueryOptions options = variantQuery.getInputOptions();
        if (!options.getBoolean(QUIET, false)) {
            logger.info("Query " + variantQuery.getQuery().toJson());
        }
//        logger.info("QueryOptions " + options.toJson());
//        logger.info("dbName " + dbName);

        Map<String, Variant> db = DummyVariantStorageEngine.VARIANTS.get(dbName);
        if (db == null) {
            throw new IllegalStateException("Database " + dbName + " not found");
        }
        HashSet<String> variantIds = new HashSet<>(variantQuery.getQuery().getAsStringList(VariantQueryUtils.ID_INTERSECT.key()));
        Predicate<Variant> filter = new VariantFilterBuilder().buildFilter(variantQuery);
        String unknownGenotype = variantQuery.getQuery()
                .getString(VariantQueryParam.UNKNOWN_GENOTYPE.key(), GenotypeClass.UNKNOWN_GENOTYPE);

        Iterator<Variant> iterator;
        if (variantIds.isEmpty()) {
            iterator = db.values().iterator();
        } else {
            iterator = Iterators.transform(variantIds.iterator(), db::get);
            iterator = Iterators.filter(iterator, Objects::nonNull);
        }
        iterator = Iterators.filter(iterator, filter::test);
        iterator = Iterators.transform(iterator, variantOrig -> {
            Variant variant = JacksonUtils.copySafe(variantOrig, Variant.class);
            if (variantQuery.getProjection() == null) {
                variant.setStudies(Collections.emptyList());
            } else {
                List<StudyEntry> studies = new ArrayList<>(variantQuery.getProjection().getStudies().size());
                for (VariantQueryProjection.StudyVariantQueryProjection studyProjection : variantQuery.getProjection().getStudies().values()) {
                    String studyName = studyProjection.getStudyMetadata().getName();
                    StudyEntry studyEntry = variant.getStudy(studyName);
                    if (studyEntry == null) {
                        continue;
                    }
                    studies.add(studyEntry);
                    if (studyProjection.getSamples().isEmpty()) {
                        studyEntry.setSamples(Collections.emptyList());
                        studyEntry.setSamplesPosition(Collections.emptyMap());
                    } else {
                        StudyEntry studyOrig = variantOrig.getStudy(studyName);
                        LinkedHashMap<String, Integer> samplesPosition = new LinkedHashMap<>(studyProjection.getSampleNames().size());
                        List<SampleEntry> samples = new ArrayList<>(studyProjection.getSampleNames().size());
                        for (String sampleName : studyProjection.getSampleNames()) {
                            SampleEntry sample = studyOrig.getSample(sampleName);
                            if (sample == null) {
                                List<String> data = new ArrayList<>(studyEntry.getSampleDataKeys().size());
                                for (int i = 0; i < studyEntry.getSampleDataKeys().size(); i++) {
                                    if (studyEntry.getSampleDataKeys().get(i).equals(VCFConstants.GENOTYPE_KEY)) {
                                        data.add(unknownGenotype);
                                    } else {
                                        data.add(VCFConstants.MISSING_VALUE_v4);
                                    }
                                }
                                sample = new SampleEntry(null, null, data);
                            }
                            if (variantQuery.getStudyQuery().isIncludeSampleId()) {
                                sample.setSampleId(sampleName);
                            }
                            samplesPosition.put(sampleName, samples.size());
                            samples.add(sample);
                        }
                        studyEntry.setSamples(samples);
                        studyEntry.setSortedSamplesPosition(samplesPosition);
                    }
                }
                variant.setStudies(studies);
            }
            return variant;
        });

        return VariantDBIterator.wrapper(iterator);
    }

    VariantDBIterator toVariantDBIterator(final List<Variant> variants) {
        return VariantDBIterator.wrapper(variants.iterator());
    }

    @Override
    public DataResult getFrequency(ParsedVariantQuery query, Region region, int regionIntervalSize) {
        return null;
    }

    @Override
    public DataResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options) {
        return null;
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options) {
        return null;
    }

    // Update methods

    @Override
    public DataResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyMetadata studyMetadata, long timestamp, QueryOptions options) {
        Map<String, Variant> db = DummyVariantStorageEngine.VARIANTS.get(dbName);
        if (db == null) {
            throw new IllegalStateException("Database " + dbName + " not found");
        }
        int updated = 0;
        for (VariantStatsWrapper statsWrapper : variantStatsWrappers) {
            Variant variant = db.get(statsWrapper.toVariant().toString());
            if (variant != null) {
                StudyEntry studyEntry = variant.getStudy(studyMetadata.getName());
                if (studyEntry != null) {
                    updated++;
                    for (VariantStats stats : statsWrapper.getCohortStats()) {
                        List<VariantStats> statsList = new ArrayList<>(studyEntry.getStats());
                        statsList.removeIf(s -> s.getCohortId().equals(stats.getCohortId()));
                        statsList.add(stats);
                        studyEntry.setStats(statsList);
                    }
                }
            }
        }
        DataResult queryResult = new DataResult();
        logger.info("Writing " + variantStatsWrappers.size() + " stats. Updated " + updated + " variants.");
        queryResult.setNumMatches(variantStatsWrappers.size());
        queryResult.setNumUpdated(updated);
        return queryResult;
    }

    @Override
    public DataResult updateAnnotations(List<VariantAnnotation> variantAnnotations, long timestamp, QueryOptions queryOptions) {
        Map<String, Variant> db = DummyVariantStorageEngine.VARIANTS.get(dbName);
        if (db == null) {
            throw new IllegalStateException("Database " + dbName + " not found");
        }
        int updated = 0;
        for (VariantAnnotation annotation : variantAnnotations) {
            String variantId = null;
            if (annotation.getAdditionalAttributes() != null) {
                AdditionalAttribute additionalAttribute = annotation.getAdditionalAttributes().get(GROUP_NAME.key());
                if (additionalAttribute != null) {
                    variantId = additionalAttribute
                            .getAttribute()
                            .get(VARIANT_ID.key());
                }
            }
            if (variantId == null) {
                variantId = new Variant(annotation.getChromosome(), annotation.getStart(),
                        annotation.getReference(), annotation.getAlternate()).toString();
            }

            Variant variant = db.get(variantId);
            if (variant != null) {
                variant.setAnnotation(annotation);
                updated++;
            }
        }
        DataResult queryResult = new DataResult();
        logger.info("Writing " + variantAnnotations.size() + " annotations. Updated " + updated + " variants.");
        queryResult.setNumMatches(variantAnnotations.size());
        queryResult.setNumUpdated(updated);
        return queryResult;
    }

    @Override
    public DataResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, long timeStamp, QueryOptions options) {
        System.out.println("Update custom annotation : " + name);
        return new DataResult();
    }

    @Override
    public VariantStorageMetadataManager getMetadataManager() {
        return metadataManager;
    }

    @Override
    public void setVariantStorageMetadataManager(VariantStorageMetadataManager variantStorageMetadataManager) {

    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

}
