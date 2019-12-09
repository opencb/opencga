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

package org.opencb.opencga.core.results;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;
import java.util.Map;

/**
 * Created on 07/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@JsonIgnoreProperties({"samples", "numTotalSamples", "numSamples", "source", "approximateCount", "approximateCountSamplingSize"})
public class VariantQueryResult<T> extends DataResult<T> {

    private static String SAMPLES = "samples";
    private static String NUM_TOTAL_SAMPLES = "numTotalSamples";
    private static String NUM_SAMPLES = "numSamples";
    private static String SOURCE = "source";
    private static String APPROXIMATE_COUNT = "approximateCount";
    private static String APPROXIMATE_COUNT_SAMPLING_SIZE = "approximateCountSamplingSize";

    public VariantQueryResult() {
    }

    public VariantQueryResult(int dbTime, int numResults, long numMatches, List<Event> events, List<T> result,
                              Map<String, List<String>> samples, String source) {
        this(dbTime, numResults, numMatches, events, result, samples, source, null, null, null);
    }

    public VariantQueryResult(int dbTime, int numResults, long numMatches, List<Event> events, List<T> result,
                              Map<String, List<String>> samples, String source, Boolean approximateCount,
                              Integer approximateCountSamplingSize, Integer numTotalSamples) {
        super(dbTime, events, numResults, result, numMatches);
        setSamples(samples);
        setSource(source);
        setApproximateCount(approximateCount);
        setApproximateCountSamplingSize(approximateCountSamplingSize);
        if (samples != null) {
            setNumSamples(samples.values().stream().mapToInt(List::size).sum());
        }
        setNumTotalSamples(numTotalSamples);
    }

    public VariantQueryResult(DataResult<T> dataResult) {
        super(dataResult.getTime(),
                dataResult.getEvents(),
                dataResult.getNumMatches(),
                dataResult.getNumInserted(),
                dataResult.getNumUpdated(),
                dataResult.getNumDeleted(),
                dataResult.getAttributes());
        setResults(dataResult.getResults());
        setNumResults(dataResult.getNumResults());
    }

    public VariantQueryResult(DataResult<T> queryResult, Map<String, List<String>> samples) {
        this(queryResult, samples, null);
    }

    public VariantQueryResult(DataResult<T> dataResult, Map<String, List<String>> samples, String source) {
        this(dataResult);
        setSamples(samples);
        if (getNumMatches() >= 0) {
            setApproximateCount(false);
        }
        if (samples != null) {
            this.setNumSamples(samples.values().stream().mapToInt(List::size).sum());
        }
        this.setNumTotalSamples(getNumSamples());
        this.setSource(source);
    }

    public Map<String, List<String>> getSamples() {
        Object o = getAttributes().get(SAMPLES);
        if (!(o instanceof Map)) {
            return null;
        } else {
            return ((Map<String, List<String>>) o);
        }
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
        return getAttributes().containsKey(APPROXIMATE_COUNT_SAMPLING_SIZE) ? getAttributes().getInt(APPROXIMATE_COUNT_SAMPLING_SIZE) : null;
    }

    public VariantQueryResult setApproximateCountSamplingSize(Integer approximateCountSamplingSize) {
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
