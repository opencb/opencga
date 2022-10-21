/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface IndividualDBAdaptor extends AnnotationSetDBAdaptor<Individual> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER_ARRAY, ""),
        UUID("uuid", TEXT, ""),
        NAME("name", TEXT, ""),
        FATHER("father", TEXT, ""),
        MOTHER("mother", TEXT, ""),
        FAMILY_IDS("familyIds", TEXT_ARRAY, ""),
        FATHER_UID("father.uid", DECIMAL, ""),
        MOTHER_UID("mother.uid", DECIMAL, ""),
        LOCATION("location", TEXT_ARRAY, ""),
        SEX("sex", OBJECT, ""),
        SEX_ID("sex.id", TEXT, ""),
        SAMPLES("samples", TEXT_ARRAY, ""),
        SAMPLES_IDS("samples.id", TEXT_ARRAY, ""),
        SAMPLE_UIDS("samples.uid", INTEGER_ARRAY, ""),
        SAMPLE_VERSION("samples.version", INTEGER, ""),
        ETHNICITY("ethnicity", OBJECT, ""),
        ETHNICITY_ID("ethnicity.id", TEXT, ""),
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_ID("status.id", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        STATUS_DESCRIPTION("status.description", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_ID("internal.status.id", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        INTERNAL_RGA("internal.rga", OBJECT, ""),
        POPULATION_NAME("population.name", TEXT, ""),
        POPULATION_SUBPOPULATION("population.subpopulation", TEXT, ""),
        POPULATION_DESCRIPTION("population.description", TEXT, ""),
        PARENTAL_CONSANGUINITY("parentalConsanguinity", BOOLEAN, ""),
        DATE_OF_BIRTH("dateOfBirth", TEXT, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        IDENTIFIERS("identifiers", TEXT_ARRAY, ""),
        RELEASE("release", INTEGER, ""), //  Release where the individual was created
        SNAPSHOT("snapshot", INTEGER, ""), // Last version of individual at release = snapshot
        VERSION("version", INTEGER, ""), // Version of the individual

        DISORDERS("disorders", TEXT_ARRAY, ""),
        DISORDERS_ID("disorders.id", TEXT, ""),
        DISORDERS_NAME("disorders.name", TEXT, ""),

        PHENOTYPES("phenotypes", TEXT_ARRAY, ""),
        PHENOTYPES_ID("phenotypes.id", TEXT, ""),
        PHENOTYPES_NAME("phenotypes.name", TEXT, ""),
        PHENOTYPES_SOURCE("phenotypes.source", TEXT, ""),

        QUALITY_CONTROL("qualityControl", TEXT_ARRAY, ""),

        KARYOTYPIC_SEX("karyotypicSex", TEXT, ""),
        LIFE_STATUS("lifeStatus", TEXT, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"

        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""), // Alias to studyId in the database. Only for the webservices.
        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        VARIABLE_SET_ID("variableSetId", DECIMAL, ""),
        ANNOTATION_SET_NAME("annotationSetName", TEXT, ""),
        ANNOTATION("annotation", TEXT, "");

        private static Map<String, QueryParams> map;

        static {
            map = new LinkedMap();
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    default boolean exists(long individualId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), individualId)).getNumMatches() > 0;
    }

    default void checkId(long individualId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (individualId < 0) {
            throw CatalogDBException.newInstance("Individual id '{}' is not valid: ", individualId);
        }

        if (!exists(individualId)) {
            throw CatalogDBException.newInstance("Indivivual id '{}' does not exist", individualId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> individual, String userId) throws CatalogDBException;

    OpenCGAResult insert(long studyId, Individual individual, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Individual> get(long individualId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    long getStudyId(long individualId) throws CatalogDBException;

    OpenCGAResult updateProjectRelease(long studyId, int release)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId          study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

    List<Individual> calculateRelationship(long studyUid, Individual proband, int maxDegree, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default void addRelativeToList(Individual individual, Family.FamiliarRelationship relation, int degree,
                                   List<Individual> individualList) {
        if (individual.getAttributes() == null) {
            individual.setAttributes(new ObjectMap());
        }
        ObjectMap params = new ObjectMap()
                .append("DEGREE", degree)
                .append("RELATION", relation);
        individual.getAttributes().put("OPENCGA_RELATIVE", params);

        individualList.add(individual);
    }

    default QueryOptions fixOptionsForRelatives(QueryOptions options) {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        QueryOptions queryOptions = new QueryOptions(options);
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            Set<String> excludeSet = new HashSet<>(options.getAsStringList(QueryOptions.EXCLUDE));
            excludeSet.remove(QueryParams.ID.key());
            excludeSet.remove(QueryParams.UUID.key());
            excludeSet.remove(QueryParams.SEX_ID.key());
            excludeSet.remove(QueryParams.FATHER.key());
            excludeSet.remove(QueryParams.MOTHER.key());

            queryOptions.put(QueryOptions.EXCLUDE, new ArrayList<>(excludeSet));
        } else {
            Set<String> includeSet = new HashSet<>(options.getAsStringList(QueryOptions.INCLUDE));
            includeSet.add(QueryParams.ID.key());
            includeSet.add(QueryParams.UUID.key());
            includeSet.add(QueryParams.SEX_ID.key());
            includeSet.add(QueryParams.FATHER.key() + "." + QueryParams.ID.key());
            includeSet.add(QueryParams.FATHER.key() + "." + QueryParams.UID.key());
            includeSet.add(QueryParams.MOTHER.key() + "." + QueryParams.ID.key());
            includeSet.add(QueryParams.MOTHER.key() + "." + QueryParams.UID.key());

            queryOptions.put(QueryOptions.INCLUDE, new ArrayList<>(includeSet));
        }

        return queryOptions;
    }

}
