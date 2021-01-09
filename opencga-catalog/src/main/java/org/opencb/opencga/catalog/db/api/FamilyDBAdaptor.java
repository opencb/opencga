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
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.*;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 03/05/17.
 */
public interface FamilyDBAdaptor extends AnnotationSetDBAdaptor<Family> {

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", INTEGER, ""),
        UUID("uuid", TEXT, ""),
        NAME("name", TEXT, ""),
        MEMBERS("members", TEXT_ARRAY, ""),
        MEMBER_UID("members.uid", INTEGER, ""),
        MEMBER_VERSION("members.version", INTEGER, ""),
        MEMBERS_PARENTAL_CONSANGUINITY("members.parentalConsanguinity", BOOLEAN, ""),
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        DESCRIPTION("description", TEXT, ""),
        EXPECTED_SIZE("expectedSize", INTEGER, ""),
        ATTRIBUTES("attributes", TEXT, ""), // "Format: <key><operation><stringValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        NATTRIBUTES("nattributes", DECIMAL, ""), // "Format: <key><operation><numericalValue> where <operation> is [<|<=|>|>=|==|!=|~|!~]"
        BATTRIBUTES("battributes", BOOLEAN, ""), // "Format: <key><operation><true|false> where <operation> is [==|!=]"
        STATUS("status", TEXT_ARRAY, ""),
        STATUS_NAME("status.name", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),
        STATUS_DESCRIPTION("status.description", TEXT, ""),
        INTERNAL_STATUS("internal.status", TEXT_ARRAY, ""),
        INTERNAL_STATUS_NAME("internal.status.name", TEXT, ""),
        INTERNAL_STATUS_MSG("internal.status.msg", TEXT, ""),
        INTERNAL_STATUS_DATE("internal.status.date", TEXT, ""),
        RELEASE("release", INTEGER, ""),
        SNAPSHOT("snapshot", INTEGER, ""), // Last version of individual at release = snapshot
        VERSION("version", INTEGER, ""), // Version of the individual

        DELETED(ParamConstants.DELETED_PARAM, BOOLEAN, ""),

        DISORDERS("disorders", TEXT_ARRAY, ""),
        DISORDERS_ID("disorders.id", TEXT, ""),

        PHENOTYPES("phenotypes", TEXT_ARRAY, ""),
        PHENOTYPES_ID("phenotypes.id", TEXT, ""),
        PHENOTYPES_NAME("phenotypes.name", TEXT, ""),
        PHENOTYPES_SOURCE("phenotypes.source", TEXT, ""),

        QUALITY_CONTROL("qualityControl", TEXT_ARRAY, ""),

        ROLES("roles", TEXT_ARRAY, ""),

        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""), // Alias to studyId in the database. Only for the webservices.

        ANNOTATION_SETS("annotationSets", TEXT_ARRAY, ""),
        ANNOTATION("annotation", TEXT_ARRAY, "");

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

    default boolean exists(long familyId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(new Query(QueryParams.UID.key(), familyId)).getNumMatches() > 0;
    }

    default void checkId(long familyId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (familyId < 0) {
            throw CatalogDBException.newInstance("Family id '{}' is not valid: ", familyId);
        }

        if (!exists(familyId)) {
            throw CatalogDBException.newInstance("Family id '{}' does not exist", familyId);
        }
    }

    OpenCGAResult nativeInsert(Map<String, Object> family, String userId) throws CatalogDBException;

    OpenCGAResult insert(long studyId, Family family, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<Family> get(long familyId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    long getStudyId(long familyId) throws CatalogDBException;

    OpenCGAResult updateProjectRelease(long studyId, int release)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    /**
     * Removes the mark of the permission rule (if existed) from all the entries from the study to notify that permission rule would need to
     * be applied.
     *
     * @param studyId study id containing the entries affected.
     * @param permissionRuleId permission rule id to be unmarked.
     * @return OpenCGAResult object.
     * @throws CatalogException if there is any database error.
     */
    OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException;

    OpenCGAResult removeMembersFromFamily(Query query, List<Long> individualUids)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    default List<Phenotype> getAllPhenotypes(List<Individual> individualList) {
        if (individualList == null || individualList.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, Phenotype> phenotypeMap = new HashMap<>();
        for (Individual individual : individualList) {
            if (individual.getPhenotypes() != null && !individual.getPhenotypes().isEmpty()) {
                for (Phenotype phenotype : individual.getPhenotypes()) {
                    phenotypeMap.put(phenotype.getId(), phenotype);
                }
            }
        }

        return new ArrayList<>(phenotypeMap.values());
    }

    default List<Disorder> getAllDisorders(List<Individual> individualList) {
        if (individualList == null || individualList.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, Disorder> disorderMap = new HashMap<>();
        for (Individual individual : individualList) {
            if (individual.getDisorders() != null && !individual.getDisorders().isEmpty()) {
                for (Disorder disorder : individual.getDisorders()) {
                    disorderMap.put(disorder.getId(), disorder);
                }
            }
        }

        return new ArrayList<>(disorderMap.values());
    }
}
