package org.opencb.opencga.storage.core.rga;

import org.apache.solr.client.solrj.beans.Field;

import java.util.List;
import java.util.Map;

public class RgaDataModel {

    @Field
    private String id;

    @Field("indId")
    private String individualId;

    @Field
    private String sampleId;

    @Field
    private String sex;

    @Field("pheno")
    private List<String> phenotypes;

    @Field("disor")
    private List<String> disorders;

    @Field
    private String geneId;

    @Field
    private String geneName;

    @Field("transcId")
    private String transcriptId;

    @Field
    private String biotype;

    @Field
    private List<String> variants;

    @Field("koTypes")
    private List<String>  knockoutTypes;

    @Field
    private List<String> filters;

    @Field("ct")
    private List<String> consequenceTypes;

    @Field("popFreqs_*")
    private Map<String, List<Float>> populationFrequencies;

    @Field("cFilters")
    private List<String> compoundFilters;

    @Field("pheJson")
    private List<String> phenotypeJson;

    @Field("disJson")
    private List<String> disorderJson;

    @Field("varJson")
    private List<String> variantJson;

    public RgaDataModel() {
    }

    public RgaDataModel(String id, String sampleId, String individualId, String sex, List<String> phenotypes, List<String> disorders,
                        String geneId, String geneName, String transcriptId, String biotype, List<String> variants,
                        List<String> knockoutTypes, List<String> filters, List<String> consequenceTypes,
                        Map<String, List<Float>> populationFrequencies, List<String> compoundFilters, List<String> phenotypeJson,
                        List<String> disorderJson, List<String> variantJson) {
        this.id = id;
        this.sampleId = sampleId;
        this.individualId = individualId;
        this.sex = sex;
        this.phenotypes = phenotypes;
        this.disorders = disorders;
        this.geneId = geneId;
        this.geneName = geneName;
        this.transcriptId = transcriptId;
        this.biotype = biotype;
        this.variants = variants;
        this.knockoutTypes = knockoutTypes;
        this.filters = filters;
        this.consequenceTypes = consequenceTypes;
        this.populationFrequencies = populationFrequencies;
        this.compoundFilters = compoundFilters;
        this.phenotypeJson = phenotypeJson;
        this.disorderJson = disorderJson;
        this.variantJson = variantJson;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RgaDataModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", sampleId='").append(sampleId).append('\'');
        sb.append(", sex='").append(sex).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", geneId='").append(geneId).append('\'');
        sb.append(", geneName='").append(geneName).append('\'');
        sb.append(", transcriptId='").append(transcriptId).append('\'');
        sb.append(", biotype='").append(biotype).append('\'');
        sb.append(", variants=").append(variants);
        sb.append(", knockoutTypes=").append(knockoutTypes);
        sb.append(", filters=").append(filters);
        sb.append(", consequenceTypes=").append(consequenceTypes);
        sb.append(", populationFrequencies=").append(populationFrequencies);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public RgaDataModel setId(String id) {
        this.id = id;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public RgaDataModel setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public RgaDataModel setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSex() {
        return sex;
    }

    public RgaDataModel setSex(String sex) {
        this.sex = sex;
        return this;
    }

    public List<String> getPhenotypes() {
        return phenotypes;
    }

    public RgaDataModel setPhenotypes(List<String> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getDisorders() {
        return disorders;
    }

    public RgaDataModel setDisorders(List<String> disorders) {
        this.disorders = disorders;
        return this;
    }

    public String getGeneId() {
        return geneId;
    }

    public RgaDataModel setGeneId(String geneId) {
        this.geneId = geneId;
        return this;
    }

    public String getGeneName() {
        return geneName;
    }

    public RgaDataModel setGeneName(String geneName) {
        this.geneName = geneName;
        return this;
    }

    public String getTranscriptId() {
        return transcriptId;
    }

    public RgaDataModel setTranscriptId(String transcriptId) {
        this.transcriptId = transcriptId;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public RgaDataModel setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public List<String> getVariants() {
        return variants;
    }

    public RgaDataModel setVariants(List<String> variants) {
        this.variants = variants;
        return this;
    }

    public List<String> getKnockoutTypes() {
        return knockoutTypes;
    }

    public RgaDataModel setKnockoutTypes(List<String> knockoutTypes) {
        this.knockoutTypes = knockoutTypes;
        return this;
    }

    public List<String> getFilters() {
        return filters;
    }

    public RgaDataModel setFilters(List<String> filters) {
        this.filters = filters;
        return this;
    }

    public List<String> getConsequenceTypes() {
        return consequenceTypes;
    }

    public RgaDataModel setConsequenceTypes(List<String> consequenceTypes) {
        this.consequenceTypes = consequenceTypes;
        return this;
    }

    public Map<String, List<Float>> getPopulationFrequencies() {
        return populationFrequencies;
    }

    public RgaDataModel setPopulationFrequencies(Map<String, List<Float>> populationFrequencies) {
        this.populationFrequencies = populationFrequencies;
        return this;
    }

    public List<String> getCompoundFilters() {
        return compoundFilters;
    }

    public RgaDataModel setCompoundFilters(List<String> compoundFilters) {
        this.compoundFilters = compoundFilters;
        return this;
    }

    public List<String> getPhenotypeJson() {
        return phenotypeJson;
    }

    public RgaDataModel setPhenotypeJson(List<String> phenotypeJson) {
        this.phenotypeJson = phenotypeJson;
        return this;
    }

    public List<String> getDisorderJson() {
        return disorderJson;
    }

    public RgaDataModel setDisorderJson(List<String> disorderJson) {
        this.disorderJson = disorderJson;
        return this;
    }

    public List<String> getVariantJson() {
        return variantJson;
    }

    public RgaDataModel setVariantJson(List<String> variantJson) {
        this.variantJson = variantJson;
        return this;
    }
}
