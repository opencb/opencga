/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class DBObjectToVariantStatsConverter implements ComplexTypeConverter<VariantStats, DBObject> {

    public static final QueryOptions STUDY_CONFIGURATION_MANAGER_QUERY_OPTIONS = new QueryOptions()
            .append(StudyConfigurationManager.CACHED, true).append(StudyConfigurationManager.READ_ONLY, true);

    public DBObjectToVariantStatsConverter() {
    }

    public DBObjectToVariantStatsConverter(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;
    }

    public static final String COHORT_ID = "cid";
    public static final String STUDY_ID = "sid";
//    public static final String FILE_ID = "fid";

    public static final String MAF_FIELD = "maf";
    public static final String MGF_FIELD = "mgf";
    public static final String MAFALLELE_FIELD = "mafAl";
    public static final String MGFGENOTYPE_FIELD = "mgfGt";
    public static final String MISSALLELE_FIELD = "missAl";
    public static final String MISSGENOTYPE_FIELD = "missGt";
    public static final String NUMGT_FIELD = "numGt";

    protected static Logger logger = LoggerFactory.getLogger(DBObjectToVariantStatsConverter.class);

    private StudyConfigurationManager studyConfigurationManager = null;
    private Map<Integer, String> studyIds = new HashMap<>();
    private Map<Integer, Map<Integer, String>> studyCohortNames = new HashMap<>();

    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;
    }

    @Override
    public VariantStats convertToDataModelType(DBObject object) {
        // Basic fields
        VariantStats stats = new VariantStats();
        stats.setMaf(((Double) object.get(MAF_FIELD)).floatValue());
        stats.setMgf(((Double) object.get(MGF_FIELD)).floatValue());
        stats.setMafAllele((String) object.get(MAFALLELE_FIELD));
        stats.setMgfGenotype((String) object.get(MGFGENOTYPE_FIELD));

        stats.setMissingAlleles((int) object.get(MISSALLELE_FIELD));
        stats.setMissingGenotypes((int) object.get(MISSGENOTYPE_FIELD));

        // Genotype counts
        BasicDBObject genotypes = (BasicDBObject) object.get(NUMGT_FIELD);
        for (Map.Entry<String, Object> o : genotypes.entrySet()) {
            String genotypeStr = o.getKey().replace("-1", ".");
            stats.addGenotype(new Genotype(genotypeStr), (int) o.getValue());
        }

        return stats;
    }

    @Override
    public DBObject convertToStorageType(VariantStats vs) {
        // Basic fields
        BasicDBObject mongoStats = new BasicDBObject(MAF_FIELD, vs.getMaf());
        mongoStats.append(MGF_FIELD, vs.getMgf());
        mongoStats.append(MAFALLELE_FIELD, vs.getMafAllele());
        mongoStats.append(MGFGENOTYPE_FIELD, vs.getMgfGenotype());
        mongoStats.append(MISSALLELE_FIELD, vs.getMissingAlleles());
        mongoStats.append(MISSGENOTYPE_FIELD, vs.getMissingGenotypes());

        // Genotype counts
        BasicDBObject genotypes = new BasicDBObject();
        for (Map.Entry<Genotype, Integer> g : vs.getGenotypesCount().entrySet()) {
            String genotypeStr = g.getKey().toString().replace(".", "-1");
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
    public void convertCohortsToDataModelType(DBObject cohortsStats, Variant variant) {
        if (cohortsStats instanceof List) {
            List<DBObject> cohortStatsList = ((List) cohortsStats);
            for (DBObject vs : cohortStatsList) {
                VariantStats variantStats = convertToDataModelType(vs);
                if (variant != null) {
                    variantStats.setRefAllele(variant.getReference());
                    variantStats.setAltAllele(variant.getAlternate());
                    variantStats.setVariantType(variant.getType());
//                    Integer fid = (Integer) vs.get(FILE_ID);
                    String sid = getStudyName((Integer) vs.get(STUDY_ID));
                    String cid = getCohortName((Integer) vs.get(STUDY_ID), (Integer) vs.get(COHORT_ID));
                    StudyEntry sourceEntry = null;
                    if (sid != null && cid != null) {
                        sourceEntry = variant.getStudiesMap().get(sid);
                        if (sourceEntry != null) {
                            sourceEntry.setStats(cid, variantStats);
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
    }

    /**
     * converts all the cohortstats within the sourceEntries.
     *
     * @param sourceEntries for instance, you can pass in variant.getSourceEntries()
     * @return list of VariantStats (as DBObjects)
     */
    public List<DBObject> convertCohortsToStorageType(Map<String, StudyEntry> sourceEntries) {
        List<DBObject> cohortsStatsList = new LinkedList<>();
        for (String studyIdFileId : sourceEntries.keySet()) {
            StudyEntry sourceEntry = sourceEntries.get(studyIdFileId);
            List<DBObject> list = convertCohortsToStorageType(sourceEntry.getStats(),
                    Integer.parseInt(sourceEntry.getStudyId()));
            cohortsStatsList.addAll(list);
        }
        return cohortsStatsList;
    }

    /**
     * converts just some cohorts stats in one VariantSourceEntry.
     *
     * @param cohortStats for instance, you can pass in sourceEntry.getStats()
     * @param studyId     of the source entry
     * @return list of VariantStats (as DBObjects)
     */
    public List<DBObject> convertCohortsToStorageType(Map<String, VariantStats> cohortStats, int studyId) {
        List<DBObject> cohortsStatsList = new LinkedList<>();
        VariantStats variantStats;
        for (Map.Entry<String, VariantStats> variantStatsEntry : cohortStats.entrySet()) {
            variantStats = variantStatsEntry.getValue();
            DBObject variantStatsDBObject = convertToStorageType(variantStats);
            Integer cohortId = getCohortId(studyId, variantStatsEntry.getKey());
            if (cohortId != null) {
                variantStatsDBObject.put(DBObjectToVariantStatsConverter.COHORT_ID, (int) cohortId);
                variantStatsDBObject.put(DBObjectToVariantStatsConverter.STUDY_ID, studyId);
                cohortsStatsList.add(variantStatsDBObject);
            }
        }
        return cohortsStatsList;
    }


    private String getStudyName(int studyId) {
        if (!studyIds.containsKey(studyId)) {
            if (studyConfigurationManager == null) {
                studyIds.put(studyId, Integer.toString(studyId));
            } else {
                QueryResult<StudyConfiguration> queryResult = studyConfigurationManager.getStudyConfiguration(studyId, null);
                if (queryResult.getResult().isEmpty()) {
                    studyIds.put(studyId, Integer.toString(studyId));
                } else {
                    studyIds.put(studyId, queryResult.first().getStudyName());
                }
            }
        }
        return studyIds.get(studyId);
    }


    private String getCohortName(int studyId, int cohortId) {
        if (studyCohortNames.containsKey(studyId)) {
            return studyCohortNames.get(studyId).get(cohortId);
        } else {
            Map<Integer, String> cohortNames = StudyConfiguration.inverseMap(getStudyConfiguration(studyId).getCohortIds());
            studyCohortNames.put(studyId, cohortNames);
            return cohortNames.get(cohortId);
        }
    }

    private Integer getCohortId(int studyId, String cohortName) {
        StudyConfiguration studyConfiguration = getStudyConfiguration(studyId);
        Map<String, Integer> cohortIds = studyConfiguration.getCohortIds();
        Integer integer = cohortIds.get(cohortName);
        return integer;
    }

    private StudyConfiguration getStudyConfiguration(int studyId) {
        return studyConfigurationManager.getStudyConfiguration(studyId, STUDY_CONFIGURATION_MANAGER_QUERY_OPTIONS).first();
    }


}

