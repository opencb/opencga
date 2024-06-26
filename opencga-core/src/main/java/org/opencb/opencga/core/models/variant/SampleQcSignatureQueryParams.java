package org.opencb.opencga.core.models.variant;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.tools.ToolParams;

public class SampleQcSignatureQueryParams extends ToolParams {

    private String sample;
    private String ct;
    private String biotype;
    private String filter;
    private String qual;
    private String region;
    private String gene;
    private String panel;

    public SampleQcSignatureQueryParams() {
    }

    public SampleQcSignatureQueryParams(Query query) {
        appendQuery(query);
    }

    public SampleQcSignatureQueryParams appendQuery(Query query) {
        updateParams(query);
        return this;
    }

    public Query toQuery() {
        return new Query(toObjectMap());
    }

    public String getSample() {
        return sample;
    }

    public SampleQcSignatureQueryParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getCt() {
        return ct;
    }

    public SampleQcSignatureQueryParams setCt(String ct) {
        this.ct = ct;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public SampleQcSignatureQueryParams setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public SampleQcSignatureQueryParams setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public SampleQcSignatureQueryParams setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public SampleQcSignatureQueryParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getGene() {
        return gene;
    }

    public SampleQcSignatureQueryParams setGene(String gene) {
        this.gene = gene;
        return this;
    }

    public String getPanel() {
        return panel;
    }

    public SampleQcSignatureQueryParams setPanel(String panel) {
        this.panel = panel;
        return this;
    }
}
