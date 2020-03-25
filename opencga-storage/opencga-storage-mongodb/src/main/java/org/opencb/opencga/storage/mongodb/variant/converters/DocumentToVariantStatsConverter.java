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

package org.opencb.opencga.storage.mongodb.variant.converters;

import htsjdk.variant.vcf.VCFConstants;
import org.bson.Document;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class DocumentToVariantStatsConverter {

    private static final Pattern MISSING_ALLELE = Pattern.compile("-1", Pattern.LITERAL);

    public static final String COHORT_ID = "cid";
    public static final String STUDY_ID = "sid";
//    public static final String FILE_ID = "fid";

    public static final String ALT_FREQ_FIELD = "af";
    public static final String REF_FREQ_FIELD = "rf";
    public static final String MAF_FIELD = "maf";
    public static final String MGF_FIELD = "mgf";
    public static final String MAFALLELE_FIELD = "mafAl";
    public static final String MGFGENOTYPE_FIELD = "mgfGt";
    public static final String MISSALLELE_FIELD = "missAl";
    public static final String MISSGENOTYPE_FIELD = "missGt";
    public static final String NUMGT_FIELD = "numGt";
    public static final String FILTER_COUNT_FIELD = "fc";
    public static final String FILTER_FREQ_FIELD = "ff";
    public static final String QUAL_AVG_FIELD = "qualAvg";

    protected static Logger logger = LoggerFactory.getLogger(DocumentToVariantStatsConverter.class);

    private VariantStorageMetadataManager variantStorageMetadataManager = null;
    private Map<Integer, String> studyIds = new HashMap<>();
    private Map<String, Genotype> genotypeMap = new HashMap<>();

    public DocumentToVariantStatsConverter() {
    }

    public DocumentToVariantStatsConverter(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.variantStorageMetadataManager = variantStorageMetadataManager;
    }

    public void setVariantStorageMetadataManager(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.variantStorageMetadataManager = variantStorageMetadataManager;
    }

    public VariantStats convertToDataModelType(Document object) {
        return convertToDataModelType(object, null);
    }

    public VariantStats convertToDataModelType(Document object, Variant variant) {
        VariantStats stats = new VariantStats();

        // Basic fields
        stats.setMaf(((Number) object.get(MAF_FIELD)).floatValue());
        stats.setMgf(((Number) object.get(MGF_FIELD)).floatValue());
        stats.setMafAllele((String) object.get(MAFALLELE_FIELD));
        stats.setMgfGenotype((String) object.get(MGFGENOTYPE_FIELD));

        stats.setMissingAlleleCount(((Number) object.get(MISSALLELE_FIELD)).intValue());
        stats.setMissingGenotypeCount(((Number) object.get(MISSGENOTYPE_FIELD)).intValue());

        // Genotype counts
        int alleleNumber = 0;
        int gtNumber = 0;
        Document genotypes = (Document) object.get(NUMGT_FIELD);
        HashMap<Genotype, Integer> genotypesCount = new HashMap<>();
        for (Map.Entry<String, Object> o : genotypes.entrySet()) {
            String genotypeStr = o.getKey();
            int value = ((Number) o.getValue()).intValue();
            Genotype g = getGenotype(genotypeStr);
            genotypesCount.put(g, value);
            if (g.getCode() != AllelesCode.ALLELES_MISSING) {
                alleleNumber += value * g.getAllelesIdx().length;
                gtNumber += value;
            }
        }
        stats.setGenotypeCount(genotypesCount);
        if (alleleNumber == 0) {
            stats.setAlleleCount(-1);
        } else {
            stats.setAlleleCount(alleleNumber);
        }

        Map<Genotype, Float> genotypesFreq;
        if (gtNumber > 0) {
            genotypesFreq = new HashMap<>();
            for (Map.Entry<Genotype, Integer> entry : genotypesCount.entrySet()) {
                if (entry.getKey().getCode() != AllelesCode.ALLELES_MISSING) {
                    genotypesFreq.put(entry.getKey(), entry.getValue().floatValue() / gtNumber);
                }
            }
        } else {
            genotypesFreq = Collections.emptyMap();
        }
        stats.setGenotypeFreq(genotypesFreq);

        Object alleleFreq = object.get(ALT_FREQ_FIELD);
        if (alleleFreq != null && ((Number) alleleFreq).floatValue() >= 0) {
            // This field is not present in files loaded before v1.3.3
            stats.setRefAlleleFreq(((Number) object.get(REF_FREQ_FIELD)).floatValue());
            stats.setAltAlleleFreq(((Number) alleleFreq).floatValue());
            if (alleleNumber == 0) {
                stats.setRefAlleleCount(-1);
                stats.setAltAlleleCount(-1);
            } else {
                stats.setRefAlleleCount(Math.round(stats.getRefAlleleFreq() * alleleNumber));
                stats.setAltAlleleCount(Math.round(stats.getAltAlleleFreq() * alleleNumber));
            }
        } else if (stats.getGenotypeCount().isEmpty()) {
            // Aggregated files usually don't have Genotype Count
            if (variant.getReference().equals(stats.getMafAllele())) {
                stats.setRefAlleleFreq(stats.getMaf());
                stats.setAltAlleleFreq(1 - stats.getMaf());
            } else {
                stats.setAltAlleleFreq(stats.getMaf());
                stats.setRefAlleleFreq(1 - stats.getMaf());
            }
        } else {
            // To calculate the alleleFrequency and so on, we need to get the alleleCounts from the genotypeCounts
            // This code should not be called with datasets loaded after v1.3.3
            int[] alleleCounts = {0, 0};
            for (Map.Entry<Genotype, Integer> entry : stats.getGenotypeCount().entrySet()) {
                for (int i : entry.getKey().getAllelesIdx()) {
                    if (i == 0 || i == 1) {
                        alleleCounts[i] += entry.getValue();
                    }
                }
            }

            stats.setRefAlleleCount(alleleCounts[0]);
            stats.setAltAlleleCount(alleleCounts[1]);
            if (alleleNumber == 0) {
                stats.setRefAlleleFreq(-1F);
                stats.setAltAlleleFreq(-1F);
            } else {
                stats.setRefAlleleFreq(alleleCounts[0] / ((float) alleleNumber));
                stats.setAltAlleleFreq(alleleCounts[1] / ((float) alleleNumber));
            }
        }

        if (object.containsKey(FILTER_COUNT_FIELD)) {
            for (Map.Entry<String, Object> entry : object.get(FILTER_COUNT_FIELD, Document.class).entrySet()) {
                stats.getFilterCount().put(entry.getKey(), ((Number)entry.getValue()).intValue());
            }
            for (Map.Entry<String, Object> entry : object.get(FILTER_FREQ_FIELD, Document.class).entrySet()) {
                stats.getFilterFreq().put(entry.getKey(), ((Number)entry.getValue()).floatValue());
            }
            if (stats.getFilterFreq().size() == 1) {
                Float freq = stats.getFilterFreq().get(VCFConstants.PASSES_FILTERS_v4);
                if (freq != null && freq == 0) {
                    stats.getFilterFreq().remove(VCFConstants.PASSES_FILTERS_v4);
                }
            }
        }
        if (object.containsKey(QUAL_AVG_FIELD)) {
            stats.setQualityAvg(object.getDouble(QUAL_AVG_FIELD).floatValue());
        }


        return stats;
    }

    private Genotype getGenotype(String genotypeStr) {
        Genotype genotype = genotypeMap.get(genotypeStr);
        if (genotype == null) {
            genotype = new Genotype(MISSING_ALLELE.matcher(genotypeStr).replaceAll("."));
            genotypeMap.put(genotypeStr, genotype);
        }
        return genotype;
    }

    public Document convertToStorageType(VariantStats vs) {
        // Basic fields
        Document mongoStats = new Document(MAF_FIELD, vs.getMaf());
        mongoStats.append(ALT_FREQ_FIELD, vs.getAltAlleleFreq());
        mongoStats.append(REF_FREQ_FIELD, vs.getRefAlleleFreq());
        mongoStats.append(MGF_FIELD, vs.getMgf());
        mongoStats.append(MAFALLELE_FIELD, vs.getMafAllele());
        mongoStats.append(MGFGENOTYPE_FIELD, vs.getMgfGenotype());
        mongoStats.append(MISSALLELE_FIELD, vs.getMissingAlleleCount());
        mongoStats.append(MISSGENOTYPE_FIELD, vs.getMissingGenotypeCount());
        vs.getFilterCount().putIfAbsent(VCFConstants.PASSES_FILTERS_v4, 0);
        mongoStats.append(FILTER_COUNT_FIELD, vs.getFilterCount());
        vs.getFilterFreq().putIfAbsent(VCFConstants.PASSES_FILTERS_v4, 0F);
        mongoStats.append(FILTER_FREQ_FIELD, vs.getFilterFreq());
        mongoStats.append(QUAL_AVG_FIELD, vs.getQualityAvg());

        // Genotype counts
        Document genotypes = new Document();
        for (Map.Entry<Genotype, Integer> g : vs.getGenotypeCount().entrySet()) {
            String genotypeStr = DocumentToSamplesConverter.genotypeToStorageType(g.getKey().toString());
            genotypes.append(genotypeStr, g.getValue());
        }
        mongoStats.append(NUMGT_FIELD, genotypes);
        return mongoStats;
    }

    /**
     * As in mongo, a variant is {studies:[],stats:[]} but the data model is {studies:[stats:[]]} this method doesn't
     * return anything. Instead, the sourceEntries within the variant is filled.
     *
     * @param cohortsStats List from mongo containing VariantStats.
     * @param variant      contains allele info to fill the VariantStats, and it sourceEntries will be filled.
     */
    public void convertCohortsToDataModelType(List<Document> cohortsStats, Variant variant) {
        if (variant != null) {
            for (Document vs : cohortsStats) {
                VariantStats variantStats = convertToDataModelType(vs, variant);
//                variantStats.setRefAllele(variant.getReference());
//                variantStats.setAltAllele(variant.getAlternate());
//                variantStats.setVariantType(variant.getType());
//                    Integer fid = (Integer) vs.get(FILE_ID);
                String sid = getStudyName((Integer) vs.get(STUDY_ID));
                String cid = getCohortName((Integer) vs.get(STUDY_ID), (Integer) vs.get(COHORT_ID));
                variantStats.setCohortId(cid);
                StudyEntry sourceEntry;
                if (sid != null && cid != null) {
                    sourceEntry = variant.getStudiesMap().get(sid);
                    if (sourceEntry != null) {
                        sourceEntry.addStats(variantStats);
                    } else {
                        //This could happen if the study has been excluded
                        logger.trace("ignoring non present source entry studyId={}", sid);
                    }
                } else {
                    logger.error("invalid mongo document: all studyId={}, cohortId={} should be present.", sid, cid);
                }
            }
        }
    }

    /**
     * converts all the cohortstats within the sourceEntries.
     *
     * @param sourceEntries for instance, you can pass in variant.getSourceEntries()
     * @return list of VariantStats (as Documents)
     */
    public List<Document> convertCohortsToStorageType(Map<String, StudyEntry> sourceEntries) {
        List<Document> cohortsStatsList = new LinkedList<>();
        for (String studyIdFileId : sourceEntries.keySet()) {
            StudyEntry sourceEntry = sourceEntries.get(studyIdFileId);
            List<Document> list = convertCohortsToStorageType(sourceEntry.getStats(),
                    Integer.parseInt(sourceEntry.getStudyId()));
            cohortsStatsList.addAll(list);
        }
        return cohortsStatsList;
    }

    /**
     * converts just some cohorts stats in one VariantSourceEntry.
     *
     * @param cohortStats for instance, you can pass in sourceEntry.stats()
     * @param studyId     of the source entry
     * @return list of VariantStats (as Documents)
     */
    public List<Document> convertCohortsToStorageType(List<VariantStats> cohortStats, int studyId) {
        List<Document> cohortsStatsList = new LinkedList<>();
        for (VariantStats variantStats : cohortStats) {
            Document variantStatsDocument = convertToStorageType(variantStats);
            Integer cohortId = getCohortId(studyId, variantStats.getCohortId());
            if (cohortId != null) {
                variantStatsDocument.put(DocumentToVariantStatsConverter.COHORT_ID, (int) cohortId);
                variantStatsDocument.put(DocumentToVariantStatsConverter.STUDY_ID, studyId);
                cohortsStatsList.add(variantStatsDocument);
            }
        }
        return cohortsStatsList;
    }


    private String getStudyName(int studyId) {
        if (!studyIds.containsKey(studyId)) {
            if (variantStorageMetadataManager == null) {
                studyIds.put(studyId, Integer.toString(studyId));
            } else {
                variantStorageMetadataManager.getStudies(null).forEach((name, id) -> studyIds.put(id, name));
            }
        }
        return studyIds.get(studyId);
    }

    private String getCohortName(int studyId, int cohortId) {
        return variantStorageMetadataManager.getCohortName(studyId, cohortId);
    }

    private Integer getCohortId(int studyId, String cohortName) {
        return variantStorageMetadataManager.getCohortId(studyId, cohortName);
    }

}

