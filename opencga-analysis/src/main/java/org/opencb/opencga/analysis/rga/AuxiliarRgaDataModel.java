package org.opencb.opencga.analysis.rga;

import org.apache.solr.client.solrj.beans.Field;

import java.util.*;

public class AuxiliarRgaDataModel {

    @Field
    private String id;

    @Field
    private String dbSnp;

    @Field
    private String type;

    @Field
    private List<String>  knockoutTypes;

    @Field
    private List<String> consequenceTypes;

    @Field(POPULATION_FREQUENCIES)
    private Map<String, String> populationFrequencies;

    @Field
    private List<String> clinicalSignificances;

    @Field
    private List<String> geneIds;

    @Field
    private List<String> geneNames;

    @Field
    private List<String> transcriptIds;

    @Field
    private List<String> compoundFilters;

    public static final String ID = "id";
    public static final String DB_SNP = "dbSnp";
    public static final String TYPE = "type";
    public static final String KNOCKOUT_TYPES = "knockoutTypes";
    public static final String CONSEQUENCE_TYPES = "consequenceTypes";
    public static final String CLINICAL_SIGNIFICANCES = "clinicalSignificances";
    public static final String POPULATION_FREQUENCIES = "populationFrequencies_*";
    public static final String GENE_IDS = "geneIds";
    public static final String GENE_NAMES = "geneNames";
    public static final String TRANSCRIPT_IDS = "transcriptIds";
    public static final String COMPOUND_FILTERS = "compoundFilters";

    public static final Map<String, String> MAIN_TO_AUXILIAR_DATA_MODEL_MAP;

    static {
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP = new HashMap<>();
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.VARIANTS.key(), ID);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.DB_SNPS.key(), DB_SNP);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.TYPE.key(), TYPE);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.KNOCKOUT.key(), KNOCKOUT_TYPES);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.CONSEQUENCE_TYPE.key(), CONSEQUENCE_TYPES);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.CLINICAL_SIGNIFICANCE.key(), CLINICAL_SIGNIFICANCES);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.POPULATION_FREQUENCY.key(), POPULATION_FREQUENCIES);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.GENE_ID.key(), GENE_IDS);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.GENE_NAME.key(), GENE_NAMES);
        MAIN_TO_AUXILIAR_DATA_MODEL_MAP.put(RgaQueryParams.TRANSCRIPT_ID.key(), TRANSCRIPT_IDS);
    }

    public AuxiliarRgaDataModel() {
    }

    public AuxiliarRgaDataModel(String id, String dbSnp, String type, List<String> knockoutTypes, List<String> consequenceTypes,
                                Map<String, String> populationFrequencies, List<String> clinicalSignificances, List<String> geneIds,
                                List<String> geneNames, List<String> transcriptIds, List<String> compoundFilters) {
        this.id = id;
        this.dbSnp = dbSnp;
        this.type = type;
        this.knockoutTypes = knockoutTypes;
        this.consequenceTypes = consequenceTypes;
        this.populationFrequencies = populationFrequencies;
        this.clinicalSignificances = clinicalSignificances;
        this.geneIds = geneIds;
        this.geneNames = geneNames;
        this.transcriptIds = transcriptIds;
        this.compoundFilters = compoundFilters;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AuxiliarRgaDataModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", dbSnp='").append(dbSnp).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", knockoutTypes=").append(knockoutTypes);
        sb.append(", consequenceTypes=").append(consequenceTypes);
        sb.append(", populationFrequencies=").append(populationFrequencies);
        sb.append(", clinicalSignificances=").append(clinicalSignificances);
        sb.append(", geneIds=").append(geneIds);
        sb.append(", geneNames=").append(geneNames);
        sb.append(", transcriptIds=").append(transcriptIds);
        sb.append(", compoundFilters=").append(compoundFilters);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public AuxiliarRgaDataModel setId(String id) {
        this.id = id;
        return this;
    }

    public String getDbSnp() {
        return dbSnp;
    }

    public AuxiliarRgaDataModel setDbSnp(String dbSnp) {
        this.dbSnp = dbSnp;
        return this;
    }

    public String getType() {
        return type;
    }

    public AuxiliarRgaDataModel setType(String type) {
        this.type = type;
        return this;
    }

    public List<String> getKnockoutTypes() {
        return knockoutTypes;
    }

    public AuxiliarRgaDataModel setKnockoutTypes(List<String> knockoutTypes) {
        this.knockoutTypes = knockoutTypes;
        return this;
    }

    public List<String> getConsequenceTypes() {
        return consequenceTypes;
    }

    public AuxiliarRgaDataModel setConsequenceTypes(List<String> consequenceTypes) {
        this.consequenceTypes = consequenceTypes;
        return this;
    }

    public Map<String, String> getPopulationFrequencies() {
        return populationFrequencies;
    }

    public AuxiliarRgaDataModel setPopulationFrequencies(Map<String, String> populationFrequencies) {
        this.populationFrequencies = populationFrequencies;
        return this;
    }

    public List<String> getClinicalSignificances() {
        return clinicalSignificances;
    }

    public AuxiliarRgaDataModel setClinicalSignificances(List<String> clinicalSignificances) {
        this.clinicalSignificances = clinicalSignificances;
        return this;
    }

    public List<String> getGeneIds() {
        return geneIds;
    }

    public AuxiliarRgaDataModel setGeneIds(List<String> geneIds) {
        this.geneIds = geneIds;
        return this;
    }

    public List<String> getGeneNames() {
        return geneNames;
    }

    public AuxiliarRgaDataModel setGeneNames(List<String> geneNames) {
        this.geneNames = geneNames;
        return this;
    }

    public List<String> getTranscriptIds() {
        return transcriptIds;
    }

    public AuxiliarRgaDataModel setTranscriptIds(List<String> transcriptIds) {
        this.transcriptIds = transcriptIds;
        return this;
    }

    public List<String> getCompoundFilters() {
        return compoundFilters;
    }

    public AuxiliarRgaDataModel setCompoundFilters(List<String> compoundFilters) {
        this.compoundFilters = compoundFilters;
        return this;
    }
}
