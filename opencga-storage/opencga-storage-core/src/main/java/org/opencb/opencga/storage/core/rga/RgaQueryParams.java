package org.opencb.opencga.storage.core.rga;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryParam;

import java.util.HashMap;
import java.util.Map;

public class RgaQueryParams implements QueryParam {

    private final String key;
    private final Type type;
    private final String description;

    private static final Map<String, RgaQueryParams> VALUES_MAP = new HashMap<>();

    public static final String SAMPLE_ID_DESCR = "Filter by sample id.";
    public static final RgaQueryParams SAMPLE_ID = new RgaQueryParams("sampleId", Type.TEXT, SAMPLE_ID_DESCR);

    public static final String INDIVIDUAL_ID_DESCR = "Filter by individual id.";
    public static final RgaQueryParams INDIVIDUAL_ID = new RgaQueryParams("individualId", Type.TEXT, INDIVIDUAL_ID_DESCR);

    public static final String SEX_DESCR = "Filter by sex.";
    public static final RgaQueryParams SEX = new RgaQueryParams("sex", Type.TEXT, SEX_DESCR);

    public static final String PHENOTYPES_DESCR = "Filter by phenotypes.";
    public static final RgaQueryParams PHENOTYPES = new RgaQueryParams("phenotypes", Type.TEXT_ARRAY, PHENOTYPES_DESCR);

    public static final String DISORDERS_DESCR = "Filter by disorders.";
    public static final RgaQueryParams DISORDERS = new RgaQueryParams("disorders", Type.TEXT_ARRAY, DISORDERS_DESCR);

    public static final String NUM_PARENTS_DESCR = "Filter by the number of parents registered.";
    public static final RgaQueryParams NUM_PARENTS = new RgaQueryParams("numParents", Type.INTEGER, NUM_PARENTS_DESCR);

    public static final String GENE_ID_DESCR = "Filter by gene id.";
    public static final RgaQueryParams GENE_ID = new RgaQueryParams("geneId", Type.TEXT, GENE_ID_DESCR);

    public static final String GENE_NAME_DESCR = "Filter by gene name.";
    public static final RgaQueryParams GENE_NAME = new RgaQueryParams("geneName", Type.TEXT, GENE_NAME_DESCR);

    public static final String GENE_BIOTYPE_DESCR = "Filter by gene biotype.";
    public static final RgaQueryParams GENE_BIOTYPE = new RgaQueryParams("geneBiotype", Type.TEXT, GENE_BIOTYPE_DESCR);

    public static final String CHROMOSOME_DESCR = "Filter by chromosome.";
    public static final RgaQueryParams CHROMOSOME = new RgaQueryParams("chromosome", Type.TEXT, CHROMOSOME_DESCR);

    public static final String START_DESCR = "Filter by start position.";
    public static final RgaQueryParams START = new RgaQueryParams("start", Type.TEXT, START_DESCR);

    public static final String END_DESCR = "Filter by end position.";
    public static final RgaQueryParams END = new RgaQueryParams("end", Type.TEXT, END_DESCR);

    public static final String TRANSCRIPT_ID_DESCR = "Filter by transcript id.";
    public static final RgaQueryParams TRANSCRIPT_ID = new RgaQueryParams("transcriptId", Type.TEXT, TRANSCRIPT_ID_DESCR);

    public static final String TRANSCRIPT_BIOTYPE_DESCR = "Filter by transcript biotype.";
    public static final RgaQueryParams TRANSCRIPT_BIOTYPE = new RgaQueryParams("transcriptBiotype", Type.TEXT, TRANSCRIPT_BIOTYPE_DESCR);

    public static final String VARIANTS_DESCR = "Filter by variant id.";
    public static final RgaQueryParams VARIANTS = new RgaQueryParams("variants", Type.TEXT_ARRAY, VARIANTS_DESCR);

    public static final String KNOCKOUT_DESCR = "Filter by knockout type.";
    public static final RgaQueryParams KNOCKOUT = new RgaQueryParams("knockoutType", Type.TEXT, KNOCKOUT_DESCR);

    public static final String FILTER_DESCR = "Filter by filter (PASS, NOT_PASS).";
    public static final RgaQueryParams FILTER = new RgaQueryParams("filter", Type.TEXT, FILTER_DESCR);

    public static final String POPULATION_FREQUENCY_DESCR = "Filter by population frequency.";
    public static final RgaQueryParams POPULATION_FREQUENCY = new RgaQueryParams("populationFrequency", Type.TEXT,
            POPULATION_FREQUENCY_DESCR);

    public static final String CONSEQUENCE_TYPE_DESCR = "Filter by consequence type.";
    public static final RgaQueryParams CONSEQUENCE_TYPE = new RgaQueryParams("consequenceType", Type.TEXT, CONSEQUENCE_TYPE_DESCR);

    public static final String INCLUDE_INDIVIDUAL_DESCR = "Include only the comma separated list of individuals to the response";
    public static final String INCLUDE_INDIVIDUAL = "includeIndividual";

    public RgaQueryParams(String key, Type type, String description) {
        this.key = key;
        this.type = type;
        this.description = description;

        if (VALUES_MAP.put(key, this) != null) {
            throw new IllegalStateException("Found two RgaQueryParams with same key '" + key + "'.");
        }
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Type type() {
        return type;
    }

    public static RgaQueryParams valueOf(String param) {
        return VALUES_MAP.get(param);
    }

    public static Query getQueryParams(Map<String, ?> options) {
        Query query = new Query();
        for (String key : options.keySet()) {
            if (VALUES_MAP.containsKey(key)) {
                query.put(key, options.get(key));
            }
        }

        // Remove from options the keys added to the query object
        for (String key : query.keySet()) {
            options.remove(key);
        }

        return query;
    }
}
