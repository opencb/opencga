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

import com.google.common.collect.BiMap;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 28/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantDBAdaptor implements VariantDBAdaptor {


    private final String dbName;
    private boolean closed = false;
    private Logger logger = LoggerFactory.getLogger(DummyVariantDBAdaptor.class);
    private static final List<String> TEMPLATES;

    static {
        TEMPLATES = new ArrayList<>();
        for (int chr = 1; chr <= 22; chr++) {
            TEMPLATES.add(chr + ":1000:A:C");
        }
        TEMPLATES.add("X:1000:A:C");
        TEMPLATES.add("Y:1000:A:C");
        TEMPLATES.add("MT:1000:A:C");
    }

    public DummyVariantDBAdaptor(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public VariantQueryResult<Variant> get(ParsedVariantQuery query, QueryOptions options) {

        List<Variant> variants = new ArrayList<>();
        iterator(query, options).forEachRemaining(variants::add);

        return new VariantQueryResult<>(0, variants.size(), variants.size(), Collections.emptyList(), variants, null,
                DummyVariantStorageEngine.STORAGE_ENGINE_ID);
    }

    @Override
    public VariantQueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize) {
        return null;
    }

    @Override
    public DataResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options) {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public DataResult<Long> count(ParsedVariantQuery query) {
        return new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList((long) TEMPLATES.size()), 1);
    }

    @Override
    public DataResult distinct(Query query, String field) {
        return null;
    }

    @Override
    public VariantDBIterator iterator(ParsedVariantQuery variantQuery, QueryOptions options) {
        logger.info("Query " + variantQuery.getQuery().toJson());
        logger.info("QueryOptions " + options.toJson());
        logger.info("dbName " + dbName);

        List<Variant> variants = new ArrayList<>(TEMPLATES.size());
        HashSet<String> variantIds = new HashSet<>(variantQuery.getQuery().getAsStringList(VariantQueryParam.ID.key()));
        for (String template : TEMPLATES) {
            if (!variantIds.isEmpty() && !variantIds.contains(template)) {
                // Skip this variant
                continue;
            }

            Variant variant = new Variant(template);


            Map<Integer, List<Integer>> returnedSamples = getReturnedSamples(variantQuery.getQuery(), options);
            returnedSamples.forEach((study, samples) -> {
                VariantStorageMetadataManager metadataManager = getMetadataManager();
                StudyMetadata sm = metadataManager.getStudyMetadata(study);
                if (metadataManager.getIndexedFiles(sm.getId()).isEmpty()) {
                    // Ignore non indexed studies
                    return; // continue
                }
                StudyEntry st = new StudyEntry(sm.getName(), Collections.emptyList(), Collections.singletonList("GT"));
                BiMap<Integer, String> samplesMap = metadataManager.getIndexedSamplesMap(sm.getId()).inverse();
                for (Integer sampleId : samples) {
                    st.addSampleData(samplesMap.get(sampleId), Collections.singletonList("0/0"));
                }
                variant.addStudyEntry(st);
                for (CohortMetadata cohort : metadataManager.getCalculatedCohorts(sm.getId())) {
                    VariantStats stats = new VariantStats(cohort.getName());
                    stats.addGenotype(new Genotype("0/0"), cohort.getSamples().size());
                    st.addStats(stats);
                }
                List<FileEntry> files = new ArrayList<>();
                for (Integer id : metadataManager.getIndexedFiles(sm.getId())) {
                    files.add(new FileEntry(id.toString(), null, Collections.emptyMap()));
                }
                st.setFiles(files);
            });
            variants.add(variant);
        }

        return toVariantDBIterator(variants);
    }

    VariantDBIterator toVariantDBIterator(final List<Variant> variants) {
        return VariantDBIterator.wrapper(variants.iterator());
    }

    @Override
    public DataResult getFrequency(Query query, Region region, int regionIntervalSize) {
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
        DataResult queryResult = new DataResult();
        logger.info("Writing " + variantStatsWrappers.size() + " statistics");
        queryResult.setNumMatches(variantStatsWrappers.size());
        queryResult.setNumUpdated(variantStatsWrappers.size());
        return queryResult;
    }

    @Override
    public DataResult updateAnnotations(List<VariantAnnotation> variantAnnotations, long timestamp, QueryOptions queryOptions) {
        return new DataResult();
    }

    @Override
    public DataResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, long timeStamp, QueryOptions options) {
        System.out.println("Update custom annotation : " + name);
        return new DataResult();
    }

    // Unsupported methods
    @Override
    public DataResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, long timestamp, QueryOptions queryOptions) {
        System.out.println("Update stats : "
                + (variantStatsWrappers.isEmpty() ? "" : variantStatsWrappers.get(0).getCohortStats().stream().map(VariantStats::getCohortId).collect(Collectors.joining(","))));

        return new DataResult();
    }

    @Override
    public VariantStorageMetadataManager getMetadataManager() {
        return new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
    }

    @Override
    public void setVariantStorageMetadataManager(VariantStorageMetadataManager variantStorageMetadataManager) {

    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

}
