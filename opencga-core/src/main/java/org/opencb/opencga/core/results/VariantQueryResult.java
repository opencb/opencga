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

import org.opencb.commons.datastore.core.DataResult;

import java.util.List;
import java.util.Map;

/**
 * Created on 07/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryResult<T> extends DataResult<T> {

    private Map<String, List<String>> samples;
    private Integer numTotalSamples;
    private Integer numSamples;
    private String source;
    private Boolean approximateCount;
    private Integer approximateCountSamplingSize;

    public VariantQueryResult() {
        this.samples = null;
    }


    public VariantQueryResult(int dbTime, int numResults, long numMatches, List<String> warnings, List<T> result,
                              Map<String, List<String>> samples, String source) {
        this(dbTime, numResults, numMatches, warnings, result, samples, source, null, null, null);
    }

    public VariantQueryResult(int dbTime, int numResults, long numMatches, List<String> warnings, List<T> result,
                              Map<String, List<String>> samples, String source, Boolean approximateCount,
                              Integer approximateCountSamplingSize, Integer numTotalSamples) {
        super(dbTime, warnings, numResults, result, numMatches);
        this.samples = samples;
        this.source = source;
        this.approximateCount = approximateCount;
        this.approximateCountSamplingSize = approximateCountSamplingSize;
        if (samples == null) {
            this.numSamples = null;
        } else {
            this.numSamples = samples.values().stream().mapToInt(List::size).sum();
        }
        this.numTotalSamples = numTotalSamples;
    }

    public VariantQueryResult(DataResult<T> queryResult, Map<String, List<String>> samples) {
        this(queryResult, samples, null);
    }

    public VariantQueryResult(DataResult<T> dataResult, Map<String, List<String>> samples, String source) {
        super(dataResult.getTime(), dataResult.getWarnings(), dataResult.getNumResults(), dataResult.getResults(),
                dataResult.getNumMatches());
        this.samples = samples;
        if (getNumMatches() >= 0) {
            approximateCount = false;
        }
        if (samples == null) {
            this.numSamples = null;
        } else {
            this.numSamples = samples.values().stream().mapToInt(List::size).sum();
        }
        this.numTotalSamples = numSamples;
        this.source = source;
    }

    public Map<String, List<String>> getSamples() {
        return samples;
    }

    public VariantQueryResult<T> setSamples(Map<String, List<String>> samples) {
        this.samples = samples;
        return this;
    }

    public Integer getNumTotalSamples() {
        return numTotalSamples;
    }

    public VariantQueryResult<T> setNumTotalSamples(int numTotalSamples) {
        this.numTotalSamples = numTotalSamples;
        return this;
    }

    public Integer getNumSamples() {
        return numSamples;
    }

    public VariantQueryResult<T> setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    public Boolean getApproximateCount() {
        return approximateCount;
    }

    public VariantQueryResult<T> setApproximateCount(Boolean approximateCount) {
        this.approximateCount = approximateCount;
        return this;
    }

    public Integer getApproximateCountSamplingSize() {
        return approximateCountSamplingSize;
    }

    public VariantQueryResult setApproximateCountSamplingSize(Integer approximateCountSamplingSize) {
        this.approximateCountSamplingSize = approximateCountSamplingSize;
        return this;
    }

    public String getSource() {
        return source;
    }

    public VariantQueryResult<T> setSource(String source) {
        this.source = source;
        return this;
    }
}
