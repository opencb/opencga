package org.opencb.opencga.storage.core.rga;

import org.apache.solr.client.solrj.beans.Field;

import java.util.List;
import java.util.Map;

public class RgaDataModel {

    @Field
    private String id;

    @Field
    private String individualId;

    @Field
    private String sampleId;

    @Field
    private String sex;

    @Field
    private List<String> phenotypes;

    @Field
    private List<String> disorders;

    @Field
    private String geneId;

    @Field
    private String geneName;

    @Field
    private String geneBiotype;

    @Field
    private String chromosome;

    @Field
    private String strand;

    @Field
    private int start;

    @Field
    private int end;

    @Field
    private String transcriptId;

    @Field
    private String transcriptBiotype;

    @Field
    private List<String> variants;

    @Field
    private List<String>  knockoutTypes;

    @Field
    private List<String> filters;

    @Field
    private List<String> consequenceTypes;

    @Field(POPULATION_FREQUENCIES)
    private Map<String, List<Float>> populationFrequencies;

    @Field
    private List<String> compoundFilters;

    @Field
    private List<String> phenotypeJson;

    @Field
    private List<String> disorderJson;

    @Field
    private List<String> variantJson;

    public static final String ID = "id";
    public static final String INDIVIDUAL_ID = "individualId";
    public static final String SAMPLE_ID = "sampleId";
    public static final String SEX = "sex";
    public static final String PHENOTYPES = "phenotypes";
    public static final String DISORDERS = "disorders";
    public static final String GENE_ID = "geneId";
    public static final String GENE_NAME = "geneName";
    public static final String GENE_BIOTYPE = "geneBiotype";
    public static final String CHROMOSOME = "chromosome";
    public static final String STRAND = "strand";
    public static final String START = "start";
    public static final String END = "end";
    public static final String TRANSCRIPT_ID = "transcriptId";
    public static final String TRANSCRIPT_BIOTYPE = "transcriptBiotype";
    public static final String VARIANTS = "variants";
    public static final String KNOCKOUT_TYPES = "knockoutTypes";
    public static final String FILTERS = "filters";
    public static final String CONSEQUENCE_TYPES = "consequenceTypes";
    public static final String POPULATION_FREQUENCIES = "populationFrequencies_*";
    public static final String COMPOUND_FILTERS = "compoundFilters";
    public static final String PHENOTYPE_JSON = "phenotypeJson";
    public static final String DISORDER_JSON = "disorderJson";
    public static final String VARIANT_JSON = "variantJson";

    public RgaDataModel() {
    }

    public RgaDataModel(String id, String individualId, String sampleId, String sex, List<String> phenotypes, List<String> disorders,
                        String geneId, String geneName, String geneBiotype, String chromosome, String strand, int start, int end,
                        String transcriptId, String transcriptBiotype, List<String> variants, List<String> knockoutTypes,
                        List<String> filters, List<String> consequenceTypes, Map<String, List<Float>> populationFrequencies,
                        List<String> compoundFilters, List<String> phenotypeJson, List<String> disorderJson, List<String> variantJson) {
        this.id = id;
        this.individualId = individualId;
        this.sampleId = sampleId;
        this.sex = sex;
        this.phenotypes = phenotypes;
        this.disorders = disorders;
        this.geneId = geneId;
        this.geneName = geneName;
        this.geneBiotype = geneBiotype;
        this.chromosome = chromosome;
        this.strand = strand;
        this.start = start;
        this.end = end;
        this.transcriptId = transcriptId;
        this.transcriptBiotype = transcriptBiotype;
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
        sb.append(", geneBiotype='").append(geneBiotype).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", strand='").append(strand).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", transcriptId='").append(transcriptId).append('\'');
        sb.append(", transcriptBiotype='").append(transcriptBiotype).append('\'');
        sb.append(", variants=").append(variants);
        sb.append(", knockoutTypes=").append(knockoutTypes);
        sb.append(", filters=").append(filters);
        sb.append(", consequenceTypes=").append(consequenceTypes);
        sb.append(", populationFrequencies=").append(populationFrequencies);
        sb.append(", compoundFilters=").append(compoundFilters);
        sb.append(", phenotypeJson=").append(phenotypeJson);
        sb.append(", disorderJson=").append(disorderJson);
        sb.append(", variantJson=").append(variantJson);
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

    public String getIndividualId() {
        return individualId;
    }

    public RgaDataModel setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public RgaDataModel setSampleId(String sampleId) {
        this.sampleId = sampleId;
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

    public String getGeneBiotype() {
        return geneBiotype;
    }

    public RgaDataModel setGeneBiotype(String geneBiotype) {
        this.geneBiotype = geneBiotype;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public RgaDataModel setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public String getStrand() {
        return strand;
    }

    public RgaDataModel setStrand(String strand) {
        this.strand = strand;
        return this;
    }

    public int getStart() {
        return start;
    }

    public RgaDataModel setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public RgaDataModel setEnd(int end) {
        this.end = end;
        return this;
    }

    public String getTranscriptId() {
        return transcriptId;
    }

    public RgaDataModel setTranscriptId(String transcriptId) {
        this.transcriptId = transcriptId;
        return this;
    }

    public String getTranscriptBiotype() {
        return transcriptBiotype;
    }

    public RgaDataModel setTranscriptBiotype(String transcriptBiotype) {
        this.transcriptBiotype = transcriptBiotype;
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
