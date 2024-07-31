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

package com.zettagenomics.opencga.enterprise.cvdb.converters;

import com.fasterxml.jackson.databind.ObjectReader;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalInterpretationSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.panel.Panel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalInterpretationConverter extends SearchConverter<Interpretation, ClinicalInterpretationSearch> {

    private ObjectReader interpretationReader;

    private static Logger logger = LoggerFactory.getLogger(ClinicalInterpretationConverter.class);

    public ClinicalInterpretationConverter() {
        this.interpretationReader = mapper.readerFor(Interpretation.class);
    }

    public ClinicalInterpretationSearch toInterpretationSearch(Interpretation interpretation, boolean primary, String studyId, List<String> viewers) throws CvdbException {
        return toInterpretationSearch(Collections.singletonList(interpretation), primary, studyId, viewers).get(0);
    }

    public List<ClinicalInterpretationSearch> toInterpretationSearch(List<Interpretation> interpretations, boolean primary, String studyId,
                                                                     List<String> viewers) throws CvdbException {
        List<ClinicalInterpretationSearch> clinicalInterpretationSearchList = new ArrayList<>();
        for (org.opencb.opencga.core.models.clinical.Interpretation interpretation : interpretations) {
            ClinicalInterpretationSearch cis = new ClinicalInterpretationSearch()
                    .setId(interpretation.getId())
                    .setCaId(interpretation.getClinicalAnalysisId())
                    .setDescription(interpretation.getDescription())
                    .setPrimary(primary)
                    .setStudyId(studyId)
                    .setViewers(viewers);

            // Panels
            if (CollectionUtils.isNotEmpty(interpretation.getPanels())) {
                // Add panel IDs
                List<String> panelsIds = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(interpretation.getPanels())) {
                    for (Panel panel : interpretation.getPanels()) {
                        if (StringUtils.isNotEmpty(panel.getId())) {
                            panelsIds.add(panel.getId());
                        }
                        if (StringUtils.isNotEmpty(panel.getName())) {
                            panelsIds.add(panel.getName());
                        }
                    }
                }
                cis.setPanelIds(panelsIds);
            }

            // Analyst
            if (interpretation.getAnalyst() != null) {
                ClinicalAnalyst analyst = interpretation.getAnalyst();
                cis.setAnalystId(analyst.getId())
                        .setAnalystName(analyst.getName())
                        .setAnalystEmail(analyst.getEmail())
                        .setAnalystAssignedBy(analyst.getAssignedBy());
                if (StringUtils.isNotEmpty(analyst.getDate())) {
                    try {
                        String solrDate = solrDateFormat.format(simpleDateFormat.parse(analyst.getDate()));
                        cis.setAnalystDate(solrDateFormat.parse(solrDate));
                    } catch (ParseException e) {
                        logger.warn("Impossible to process interpretation analyst date {}: {}", analyst.getDate(), e.getMessage());
                    }
                }
            }

            // Method
            if (interpretation.getMethod() != null) {
                InterpretationMethod method = interpretation.getMethod();
                cis.setMethodName(method.getName())
                        .setMethodCommit(method.getCommit())
                        .setMethodVersion(method.getVersion());
                if (CollectionUtils.isNotEmpty(method.getDependencies())) {
                    cis.setMethodDependencies(method.getDependencies().stream()
                            .map(dp -> dp.getName() + ConverterUtils.FIELD_SEPARATOR + dp.getVersion())
                            .collect(Collectors.toList()));
                }
            }

            // Comments are stores: author -- message -- tag1:tag2:.. -- date -- -->
            if (CollectionUtils.isNotEmpty(interpretation.getComments())) {
                cis.setComments(interpretation.getComments().stream().map(c -> ConverterUtils.encodeComent(c))
                        .collect(Collectors.toList()));
            }

            cis.setLocked(interpretation.isLocked());

            // Status
            if (interpretation.getStatus() != null) {
                Status status = interpretation.getStatus();
                cis.setStatusId(status.getId())
                        .setStatusName(status.getName())
                        .setStatusDescription(status.getDescription());
                if (StringUtils.isNotEmpty(status.getDate())) {
                    try {
                        String solrDate = solrDateFormat.format(simpleDateFormat.parse(status.getDate()));
                        cis.setStatusDate(solrDateFormat.parse(solrDate));
                    } catch (ParseException e) {
                        logger.warn("Impossible to process interpretation status date {}: {}", status.getDate(), e.getMessage());
                    }
                }
            }

            if (StringUtils.isNotEmpty(interpretation.getCreationDate())) {
                try {
                    String solrDate = solrDateFormat.format(simpleDateFormat.parse(interpretation.getCreationDate()));
                    cis.setCreationDate(solrDateFormat.parse(solrDate));
                } catch (ParseException e) {
                    logger.warn("Impossible to process interpretation creation date {}: {}", interpretation.getCreationDate(), e.getMessage());
                }
            }

            if (StringUtils.isNotEmpty(interpretation.getModificationDate())) {
                try {
                    String solrDate = solrDateFormat.format(simpleDateFormat.parse(interpretation.getModificationDate()));
                    cis.setModificationDate(solrDateFormat.parse(solrDate));
                } catch (ParseException e) {
                    logger.warn("Impossible to process interpretation modification date {}: {}", interpretation.getModificationDate(),
                            e.getMessage());
                }
            }

            cis.setVersion(interpretation.getVersion());

            // Interpretation stored in a JSON string
            try {
                String json = mapper.writeValueAsString(interpretation);
                cis.setMaxJson(json);

                // Clinical analysis copy
                Interpretation copy = interpretationReader.readValue(json);

                // Medium JSON
                //  - minimizing panels
                if (CollectionUtils.isNotEmpty(copy.getPanels())) {
                    minimizePanels(copy.getPanels());
                }

                json = mapper.writeValueAsString(copy);
                cis.setMediumJson(json);

                // Minimum JSON
                //  - minimizing primary and secondary findings, i.e., remove variant annotation
                minimizeClinicalVariants(copy.getPrimaryFindings());
                minimizeClinicalVariants(copy.getSecondaryFindings());

                json = mapper.writeValueAsString(copy);
                cis.setMinJson(json);
            } catch (IOException e) {
                throw new CvdbException("Error when storing clinical interpretation JSON fields", e);
            }

            // Add the new interpretation search model to the list
            clinicalInterpretationSearchList.add(cis);
        }
        return clinicalInterpretationSearchList;
    }

    public Interpretation toInterpretation(ClinicalInterpretationSearch cis) throws CvdbException {
        Interpretation ci;
        if (StringUtils.isNotEmpty(cis.getMaxJson())) {
            // Build clinical interpretation from the field 'maxJson'
            try {
                ci = interpretationReader.readValue(cis.getMaxJson());
            } catch (IOException e) {
                throw new CvdbException("Error when converting to clinical interpretation from the field maxJson", e);
            }
        } else if (StringUtils.isNotEmpty(cis.getMediumJson())) {
            // Build clinical interpretation from the field 'mediumJson'
            try {
                ci = interpretationReader.readValue(cis.getMediumJson());
            } catch (IOException e) {
                throw new CvdbException("Error when converting to clinical interpretation from the field mediumJson", e);
            }
        } else if (StringUtils.isNotEmpty(cis.getMinJson())) {
            // Build clinical interpretation from the field 'minJson'
            try {
                ci = interpretationReader.readValue(cis.getMinJson());
            } catch (IOException e) {
                throw new CvdbException("Error when converting to clinical interpretation from the field minJson", e);
            }
        }  else {
            // Build clinical interpretation from other fields
            ci = new org.opencb.opencga.core.models.clinical.Interpretation();
            ci.setId(cis.getId());
            //ci.setPrimary(cis.isPrimary());
            ci.setDescription(cis.getDescription());

            // Panels
            if (CollectionUtils.isNotEmpty(cis.getPanelIds())) {
                List<Panel> panels = new ArrayList<>();
                for (String panelId : cis.getPanelIds()) {
                    panels.add(new Panel().setId(panelId));
                }
                ci.setPanels(panels);
            }

            // Analyst
            ClinicalAnalyst analyst = new ClinicalAnalyst()
                    .setId(cis.getAnalystId())
                    .setName(cis.getAnalystName())
                    .setEmail(cis.getAnalystEmail())
                    .setAssignedBy(cis.getAnalystAssignedBy());
            if (cis.getAnalystDate() != null) {
                analyst.setDate(simpleDateFormat.format(cis.getAnalystDate()));
            }
            ci.setAnalyst(analyst);

            // Method
            InterpretationMethod method = new InterpretationMethod()
                    .setName(cis.getMethodName())
                    .setVersion(cis.getMethodVersion())
                    .setCommit(cis.getMethodCommit());
            if (CollectionUtils.isNotEmpty(cis.getMethodDependencies())) {
                List<Software> dependencies = new ArrayList<>();
                for (String methodDependency : cis.getMethodDependencies()) {
                    String[] split = methodDependency.split(ConverterUtils.FIELD_SEPARATOR);
                    if (split.length > 0) {
                        Software software = new Software().setName(split[0]);
                        if (split.length > 1) {
                            software.setVersion(split[1]);
                        }
                        dependencies.add(software);
                    }
                }
                if (CollectionUtils.isNotEmpty(dependencies)) {
                    method.setDependencies(dependencies);
                }
            }
            ci.setMethod(method);

            // Comments
            if (CollectionUtils.isNotEmpty(cis.getComments())) {
                ci.setComments(cis.getComments().stream().map(s -> ConverterUtils.decodeComment(s)).collect(Collectors.toList()));
            }

            ci.setLocked(cis.isLocked());

            // Status
            Status status = new Status()
                    .setId(cis.getStatusId())
                    .setName(cis.getStatusName())
                    .setDescription(cis.getStatusDescription());
            if (cis.getStatusDate() != null) {
                status.setDate(simpleDateFormat.format(cis.getStatusDate()));
            }
            ci.setStatus(status);

            if (cis.getCreationDate() != null) {
                ci.setCreationDate(simpleDateFormat.format(cis.getCreationDate()));
            }

            if (cis.getModificationDate() != null) {
                ci.setModificationDate(simpleDateFormat.format(cis.getModificationDate()));
            }

            ci.setVersion(cis.getVersion());
        }

        return ci;
    }

    @Override
    public org.opencb.opencga.core.models.clinical.Interpretation toModel(ClinicalInterpretationSearch input) throws CvdbException {
        return toInterpretation(input);
    }
}

