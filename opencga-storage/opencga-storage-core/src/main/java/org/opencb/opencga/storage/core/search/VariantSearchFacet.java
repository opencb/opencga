package org.opencb.opencga.storage.core.search;

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
