/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.storage.core.variant.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLE_METADATA;

/**
 * Created on 07/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@JsonIgnoreProperties({"samples", "numTotalSamples", "numSamples", "source",
        "approximateCount",
        "approximateCountSamplingSize",
        "fetchTime",
        "convertTime"})
public class VariantQueryResult<T> extends OpenCGAResult<T> {

    private static final String SAMPLES = "samples";
    private static final String FETCH_TIME = "fetchTime";
    private static final String CONVERT_TIME = "convertTime";
    private static final String NUM_TOTAL_SAMPLES = "numTotalSamples";
    private static final String NUM_SAMPLES = "numSamples";
    private static final String SOURCE = "source";
    private static final String APPROXIMATE_COUNT = "approximateCount";
    private static final String APPROXIMATE_COUNT_SAMPLING_SIZE = "approximateCountSamplingSize";

    protected VariantQueryResult() {
    }

    public VariantQueryResult(long time, int numResults, long numMatches, List<Event> events, List<T> result, String source,
                              ParsedVariantQuery variantQuery) {
        this(time, numResults, numMatches, events, result, source, null, null, null, variantQuery);
    }

    public VariantQueryResult(long time, int numResults, long numMatches, List<Event> events, List<T> result,
                              String source, Boolean approximateCount, Integer approximateCountSamplingSize, Integer numTotalSamples,
                              ParsedVariantQuery variantQuery) {
        super((int) time, events, numResults, result, numMatches);

        if (source != null) {
            setSource(source);
        }
        if (approximateCount != null) {
            setApproximateCount(approximateCount);
        }
        if (approximateCountSamplingSize != null) {
            setApproximateCountSamplingSize(approximateCountSamplingSize);
        }
        if (numTotalSamples != null) {
            setNumTotalSamples(numTotalSamples);
        }
        if (variantQuery != null) {
            addSamplesMetadataIfRequested(variantQuery);
        }
    }

    private VariantQueryResult(DataResult<?> dataResult, List<T> results) {
        super(dataResult.getTime(),
                dataResult.getEvents(),
                dataResult.getNumMatches(),
                dataResult.getNumInserted(),
                dataResult.getNumUpdated(),
                dataResult.getNumDeleted(),
                dataResult.getNumErrors(),
                dataResult.getAttributes());
        setResults(results);
        setNumResults(results.size());
    }

    public VariantQueryResult(DataResult<T> dataResult, ParsedVariantQuery variantQuery) {
        this(dataResult, dataResult.getResults());
        if (variantQuery != null) {
            addSamplesMetadataIfRequested(variantQuery);
        }
    }

    public VariantQueryResult(VariantQueryResult<?> dataResult, List<T> results) {
        this((DataResult<?>) dataResult, results);
    }

    public VariantQueryResult(DataResult<T> dataResult, String source, ParsedVariantQuery variantQuery) {
        this(dataResult, variantQuery);
        setSource(source);
    }

    /*
     * @deprecated Missing ParsedVariantQuery.
     * Use {@link #VariantQueryResult(long, int, long, List, List, String, Boolean, Integer, Integer, ParsedVariantQuery)}
     */
    @Deprecated
    public VariantQueryResult(long time, int numResults, long numMatches, List<Event> events, List<T> result, String source) {
        this(time, numResults, numMatches, events, result, source, null, null, null, (ParsedVariantQuery) null);
    }

    /*
     * @deprecated Missing ParsedVariantQuery.
     * Use {@link #VariantQueryResult(DataResult, ParsedVariantQuery)}
     */
    @Deprecated
    public VariantQueryResult(DataResult<T> dataResult) {
        this(dataResult, (ParsedVariantQuery) null);
    }

    private void addSamplesMetadataIfRequested(ParsedVariantQuery query) {
        VariantQueryProjection projection = query.getProjection();

        if (!query.getEvents().isEmpty()) {
            if (getEvents() == null) {
                setEvents(new ArrayList<>());
            }
            getEvents().addAll(query.getEvents());
        }
        if (!projection.getEvents().isEmpty()) {
            if (getEvents() == null) {
                setEvents(new ArrayList<>());
            }
            getEvents().addAll(projection.getEvents());
        }

        int numTotalSamples = projection.getNumTotalSamples();
        int numSamples = projection.getNumSamples();
        if (query.getInputQuery().getBoolean(SAMPLE_METADATA.key(), false)) {
            Map<String, List<String>> samplesMetadata = query.getProjection().getSampleNames();
            if (numTotalSamples < 0 && numSamples < 0) {
                numTotalSamples = samplesMetadata.values().stream().mapToInt(List::size).sum();
                VariantQueryProjectionParser.skipAndLimitSamples(query.getQuery(), samplesMetadata);
                numSamples = samplesMetadata.values().stream().mapToInt(List::size).sum();
            }
            setNumSamples(numSamples);
            setNumTotalSamples(numTotalSamples);
            setSamples(samplesMetadata);
        } else {
            if (numTotalSamples >= 0 && numSamples >= 0) {
                setNumSamples(numSamples);
                setNumTotalSamples(numTotalSamples);
            }
        }
    }

    public Map<String, List<String>> getSamples() {
        Object o = getAttributes().get(SAMPLES);
        if (!(o instanceof Map)) {
            return null;
        } else {
            return ((Map<String, List<String>>) o);
        }
    }

    public Long getFetchTime() {
        return getAttributes().containsKey(FETCH_TIME) ? getAttributes().getLong(FETCH_TIME) : null;
    }

    public VariantQueryResult<T> setFetchTime(long fetchTime) {
        getAttributes().put(FETCH_TIME, fetchTime);
        return this;
    }

    public Long getConvertTime() {
        return getAttributes().containsKey(CONVERT_TIME) ? getAttributes().getLong(CONVERT_TIME) : null;
    }

    public VariantQueryResult<T> setConvertTime(long convertTime) {
        getAttributes().put(CONVERT_TIME, convertTime);
        return this;
    }

    public VariantQueryResult<T> setSamples(Map<String, List<String>> samples) {
        getAttributes().put(SAMPLES, samples);
        return this;
    }

    public Integer getNumTotalSamples() {
        return getAttributes().containsKey(NUM_TOTAL_SAMPLES) ? getAttributes().getInt(NUM_TOTAL_SAMPLES) : null;
    }

    public VariantQueryResult<T> setNumTotalSamples(Integer numTotalSamples) {
        getAttributes().put(NUM_TOTAL_SAMPLES, numTotalSamples);
        return this;
    }

    public Integer getNumSamples() {
        return getAttributes().containsKey(NUM_SAMPLES) ? getAttributes().getInt(NUM_SAMPLES) : null;
    }

    public VariantQueryResult<T> setNumSamples(Integer numSamples) {
        getAttributes().put(NUM_SAMPLES, numSamples);
        return this;
    }

    public Boolean getApproximateCount() {
        return getAttributes().containsKey(APPROXIMATE_COUNT) ? getAttributes().getBoolean(APPROXIMATE_COUNT) : null;
    }

    public VariantQueryResult<T> setApproximateCount(Boolean approximateCount) {
        getAttributes().put(APPROXIMATE_COUNT, approximateCount);
        return this;
    }

    public Integer getApproximateCountSamplingSize() {
        return getAttributes().containsKey(APPROXIMATE_COUNT_SAMPLING_SIZE)
                ? getAttributes().getInt(APPROXIMATE_COUNT_SAMPLING_SIZE)
                : null;
    }

    public VariantQueryResult<T> setApproximateCountSamplingSize(Integer approximateCountSamplingSize) {
        getAttributes().put(APPROXIMATE_COUNT_SAMPLING_SIZE, approximateCountSamplingSize);
        return this;
    }

    public String getSource() {
        return getAttributes().getString(SOURCE);
    }

    public VariantQueryResult<T> setSource(String source) {
        getAttributes().put(SOURCE, source);
        return this;
    }

    @Override
    public ObjectMap getAttributes() {
        ObjectMap attributes = super.getAttributes();
        if (attributes == null) {
            attributes = new ObjectMap();
            setAttributes(attributes);
        }
        return attributes;
    }
}
