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

package org.opencb.opencga.storage.core.search.solr;

import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.IntervalFacet;
import org.apache.solr.client.solrj.response.RangeFacet;

import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 14/11/16.
 */
public class VariantSearchFacet {

    private List<FacetField> facetFields;
    private List<RangeFacet> facetRanges;
    private List<IntervalFacet> facetIntervales;
    private Map<String, Integer> facetQueries;

    public VariantSearchFacet() {
    }

    public VariantSearchFacet(List<FacetField> facetFields, List<RangeFacet> facetRanges, List<IntervalFacet> facetIntervales) {
        this.facetFields = facetFields;
        this.facetRanges = facetRanges;
        this.facetIntervales = facetIntervales;
    }

    public List<FacetField> getFacetFields() {
        return facetFields;
    }

    public VariantSearchFacet setFacetFields(List<FacetField> facetFields) {
        this.facetFields = facetFields;
        return this;
    }

    public List<RangeFacet> getFacetRanges() {
        return facetRanges;
    }

    public VariantSearchFacet setFacetRanges(List<RangeFacet> facetRanges) {
        this.facetRanges = facetRanges;
        return this;
    }

    public List<IntervalFacet> getFacetIntervales() {
        return facetIntervales;
    }

    public VariantSearchFacet setFacetIntervales(List<IntervalFacet> facetIntervales) {
        this.facetIntervales = facetIntervales;
        return this;
    }

    public Map<String, Integer> getFacetQueries() {
        return facetQueries;
    }

    public VariantSearchFacet setFacetQueries(Map<String, Integer> facetQueries) {
        this.facetQueries = facetQueries;
        return this;
    }

    @Override
    public String toString() {
        return "VariantSearchFacet{"
                + "facetFields=" + facetFields
                + ", facetRanges=" + facetRanges
                + ", facetIntervales=" + facetIntervales
                + ", facetQueries=" + facetQueries
                + '}';
    }
}
