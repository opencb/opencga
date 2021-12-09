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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by wasim on 27/06/18.
 */
public class CatalogSampleToSolrSampleConverter implements ComplexTypeConverter<Sample, SampleSolrModel> {

    private Study study;
    private Map<String, Map<String, QueryParam.Type>> variableMap;

    protected static Logger logger = LoggerFactory.getLogger(CatalogSampleToSolrSampleConverter.class);

    public CatalogSampleToSolrSampleConverter(Study study) {
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
    public Sample convertToDataModelType(SampleSolrModel sampleSolrModel) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public SampleSolrModel convertToStorageType(Sample sample) {
        SampleSolrModel sampleSolrModel = new SampleSolrModel();

        sampleSolrModel.setId(sample.getUuid());
        sampleSolrModel.setUid(sample.getUid());
        sampleSolrModel.setStudyId(study.getFqn().replace(":", "__"));

        sampleSolrModel.setRelease(sample.getRelease());
        sampleSolrModel.setVersion(sample.getVersion());

        Date date = TimeUtils.toDate(sample.getCreationDate());
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        sampleSolrModel.setCreationYear(localDate.getYear());
        sampleSolrModel.setCreationMonth(localDate.getMonth().toString());
        sampleSolrModel.setCreationDay(localDate.getDayOfMonth());
        sampleSolrModel.setCreationDayOfWeek(localDate.getDayOfWeek().toString());
        sampleSolrModel.setStatus(sample.getInternal().getStatus().getName());
        sampleSolrModel.setSomatic(sample.isSomatic());

        if (sample.getPhenotypes() != null) {
            sampleSolrModel.setPhenotypes(SolrConverterUtil.populatePhenotypes(sample.getPhenotypes()));
        }

        sampleSolrModel.setAnnotations(SolrConverterUtil.populateAnnotations(variableMap, sample.getAnnotationSets()));
        if (sample.getAnnotationSets() != null) {
            sampleSolrModel.setAnnotationSets(sample.getAnnotationSets().stream().map(AnnotationSet::getId).collect(Collectors.toList()));
        } else {
            sampleSolrModel.setAnnotationSets(Collections.emptyList());
        }

        if (sample.getCollection() != null) {
//            sampleSolrModel.setTissue(StringUtils.defaultIfEmpty(sample.getCollection().getTissue(), ""));
//            sampleSolrModel.setOrgan(StringUtils.defaultIfEmpty(sample.getCollection().getOrgan(), ""));
            sampleSolrModel.setMethod(StringUtils.defaultIfEmpty(sample.getCollection().getMethod(), ""));
        }

        if (sample.getProcessing() != null) {
            sampleSolrModel.setProduct(CollectionUtils.isNotEmpty(sample.getProcessing().getProduct())
                    ? sample.getProcessing().getProduct().get(0).getId()
                    : "");
            sampleSolrModel.setPreparationMethod(StringUtils.defaultIfEmpty(sample.getProcessing().getPreparationMethod(), ""));
            sampleSolrModel.setExtractionMethod(StringUtils.defaultIfEmpty(sample.getProcessing().getExtractionMethod(), ""));
            sampleSolrModel.setLabSampleId(StringUtils.defaultIfEmpty(sample.getProcessing().getLabSampleId(), ""));
        }

        // Extract the permissions
        Map<String, Set<String>> sampleAcl =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>) sample.getAttributes().get("OPENCGA_ACL"));
        List<String> effectivePermissions =
                SolrConverterUtil.getEffectivePermissions((Map<String, Set<String>>) study.getAttributes().get("OPENCGA_ACL"), sampleAcl,
                        "SAMPLE");
        sampleSolrModel.setAcl(effectivePermissions);

        return sampleSolrModel;
    }

}
