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
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

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
    public VariantQueryResult<Variant> get(Query query, QueryOptions options) {
        logger.info("Query " + query.toJson());
        logger.info("QueryOptions " + options.toJson());
        logger.info("dbName " + dbName);

        List<Variant> variants = new ArrayList<>();
        iterator(query, options).forEachRemaining(variants::add);

        return new VariantQueryResult<>("", 0, variants.size(), variants.size(), "", "", variants, null,
                DummyVariantStorageEngine.STORAGE_ENGINE_ID);
    }

    @Override
    public List<VariantQueryResult<Variant>> get(List<Query> queries, QueryOptions options) {
        return null;
    }

    @Override
    public VariantQueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize) {
        return null;
    }

    @Override
    public QueryResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options) {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public QueryResult<Long> count(Query query) {
        return new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList((long) TEMPLATES.size()));
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        return null;
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        List<Variant> variants = new ArrayList<>(TEMPLATES.size());
        for (String template : TEMPLATES) {
            Variant variant = new Variant(template);

            Map<Integer, List<Integer>> returnedSamples = getReturnedSamples(query, options);
            returnedSamples.forEach((study, samples) -> {
                StudyConfiguration sc = getStudyConfigurationManager().getStudyConfiguration(study, null).first();
                if (sc.getIndexedFiles().isEmpty()) {
                    // Ignore non indexed studies
                    return; // continue
                }
                StudyEntry st = new StudyEntry(sc.getStudyName(), Collections.emptyList(), Collections.singletonList("GT"));
                BiMap<Integer, String> samplesMap = sc.getSampleIds().inverse();
                for (Integer sampleId : samples) {
                    st.addSampleData(samplesMap.get(sampleId), Collections.singletonList("0/0"));
                }
                variant.addStudyEntry(st);
                for (Integer cid : sc.getCalculatedStats()) {
                    VariantStats stats = new VariantStats();
                    stats.addGenotype(new Genotype("0/0"), sc.getCohorts().get(cid).size());
                    st.setStats(sc.getCohortIds().inverse().get(cid), stats);
                }
                List<FileEntry> files = new ArrayList<>();
                for (Integer id : sc.getIndexedFiles()) {
                    files.add(new FileEntry(id.toString(), "", Collections.emptyMap()));
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
    public QueryResult getFrequency(Query query, Region region, int regionIntervalSize) {
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        return null;
    }

    // Update methods

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration, QueryOptions options) {
        QueryResult queryResult = new QueryResult();
        logger.info("Writing " + variantStatsWrappers.size() + " statistics");
        queryResult.setNumResults(variantStatsWrappers.size());
        queryResult.setNumTotalResults(variantStatsWrappers.size());
        return queryResult;
    }

    @Override
    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {
        return new QueryResult();
    }

    @Override
    public QueryResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, QueryOptions options) {
        System.out.println("Update custom annotation : " + name);
        return new QueryResult();
    }

    // Unsupported methods
    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StudyConfigurationManager getStudyConfigurationManager() {
        return new StudyConfigurationManager(new DummyProjectMetadataAdaptor(), new DummyStudyConfigurationAdaptor(), new DummyVariantFileMetadataDBAdaptor());
    }

    @Override
    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {

    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

}
