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

package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.stats.solr.CohortSolrModel;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.study.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by wasim on 03/07/18.
 */
public class CatalogCohortToSolrCohortConverter implements ComplexTypeConverter<Cohort, CohortSolrModel> {

    private Study study;
    private Map<String, Map<String, QueryParam.Type>> variableMap;

    protected static Logger logger = LoggerFactory.getLogger(CatalogCohortToSolrCohortConverter.class);

    public CatalogCohortToSolrCohortConverter(Study study) {
        this.study = study;
        this.variableMap = new HashMap<>();
        if (this.study.getVariableSets() != null) {
            this.study.getVariableSets().forEach(variableSet -> {
                try {
                    this.variableMap.put(variableSet.getId(), AnnotationUtils.getVariableMap(variableSet));
                } catch (CatalogDBException e) {
                    logger.warn("Could not parse variableSet {}: {}", variableSet.getId(), e.getMessage(), e);
                }
            });
        }
    }

    @Override
    public Cohort convertToDataModelType(CohortSolrModel object) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public CohortSolrModel convertToStorageType(Cohort cohort) {
        CohortSolrModel cohortSolrModel = new CohortSolrModel();

        cohortSolrModel.setId(cohort.getUuid());
        cohortSolrModel.setUid(cohort.getUid());
        cohortSolrModel.setStudyId(study.getFqn().replace(":", "__"));
        cohortSolrModel.setType(cohort.getType().name());

        Date date = TimeUtils.toDate(cohort.getCreationDate());
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        cohortSolrModel.setCreationYear(localDate.getYear());
        cohortSolrModel.setCreationMonth(localDate.getMonth().toString());
        cohortSolrModel.setCreationDay(localDate.getDayOfMonth());
        cohortSolrModel.setCreationDayOfWeek(localDate.getDayOfWeek().toString());
        cohortSolrModel.setStatus(cohort.getInternal().getStatus().getName());

        if (cohort.getSamples() != null) {
            cohortSolrModel.setNumSamples(cohort.getSamples().size());
        } else {
            cohortSolrModel.setNumSamples(0);
        }

        cohortSolrModel.setRelease(cohort.getRelease());
        cohortSolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(variableMap, cohort.getAnnotationSets()));

        if (cohort.getAnnotationSets() != null) {
            cohortSolrModel.setAnnotationSets(cohort.getAnnotationSets().stream().map(AnnotationSet::getId).collect(Collectors.toList()));
        } else {
            cohortSolrModel.setAnnotationSets(Collections.emptyList());
        }

        // Extract the permissions
        Map<String, Set<String>> cohortAcl =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) cohort.getAttributes().get("OPENCGA_ACL"));
        List<String> effectivePermissions =
                SolrConverterUtil.getEffectivePermissions((Map<String, Set<String>>) study.getAttributes().get("OPENCGA_ACL"), cohortAcl,
                        "COHORT");
        cohortSolrModel.setAcl(effectivePermissions);

        return cohortSolrModel;
    }
}
