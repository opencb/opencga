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
import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.cvdb.converters.ConverterUtils.decompressFromBase64;

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
            ClinicalInterpretationSearch clinicalInterpretationSearch = new ClinicalInterpretationSearch()
                    .setId(interpretation.getId())
                    .setCaId(interpretation.getClinicalAnalysisId())
                    .setDescription(interpretation.getDescription())
                    .setPrimary(primary)
                    .setStudyId(studyId)
                    .setViewers(viewers);

            // Panels
            if (CollectionUtils.isNotEmpty(interpretation.getPanels())) {
                // Add panel IDs and names
                clinicalInterpretationSearch.setPanelIds(interpretation.getPanels().stream().map(p -> p.getId())
                        .collect(Collectors.toList()));
                clinicalInterpretationSearch.getPanelIds().addAll(interpretation.getPanels().stream().map(p -> p.getName())
                        .collect(Collectors.toList()));
            }

            // Analyst
            if (interpretation.getAnalyst() != null) {
                ClinicalAnalyst analyst = interpretation.getAnalyst();
                clinicalInterpretationSearch.setAnalystId(analyst.getId())
                        .setAnalystName(analyst.getName())
                        .setAnalystEmail(analyst.getEmail())
                        .setAnalystAssignedBy(analyst.getAssignedBy());
                if (StringUtils.isNotEmpty(analyst.getDate())) {
                    try {
                        String solrDate = solrDateFormat.format(simpleDateFormat.parse(analyst.getDate()));
                        clinicalInterpretationSearch.setAnalystDate(solrDateFormat.parse(solrDate));
                    } catch (ParseException e) {
                        logger.warn("Impossible to process interpretation analyst date {}: {}", analyst.getDate(), e.getMessage());
                    }
                }
            }

            // Method
            if (interpretation.getMethod() != null) {
                InterpretationMethod method = interpretation.getMethod();
                clinicalInterpretationSearch.setMethodName(method.getName())
                        .setMethodCommit(method.getCommit())
                        .setMethodVersion(method.getVersion());
                if (CollectionUtils.isNotEmpty(method.getDependencies())) {
                    clinicalInterpretationSearch.setMethodDependencies(method.getDependencies().stream()
                            .map(dp -> dp.getName() + ConverterUtils.FIELD_SEPARATOR + dp.getVersion())
                            .collect(Collectors.toList()));
                }
            }

            // Comments are stores: author -- message -- tag1:tag2:.. -- date -- -->
            if (CollectionUtils.isNotEmpty(interpretation.getComments())) {
                clinicalInterpretationSearch.setComments(interpretation.getComments().stream().map(c -> ConverterUtils.encodeComent(c))
                        .collect(Collectors.toList()));
            }

            clinicalInterpretationSearch.setLocked(interpretation.isLocked());

            // Status
            if (interpretation.getStatus() != null) {
                Status status = interpretation.getStatus();
                clinicalInterpretationSearch.setStatusId(status.getId())
                        .setStatusName(status.getName())
                        .setStatusDescription(status.getDescription());
                if (StringUtils.isNotEmpty(status.getDate())) {
                    try {
                        String solrDate = solrDateFormat.format(simpleDateFormat.parse(status.getDate()));
                        clinicalInterpretationSearch.setStatusDate(solrDateFormat.parse(solrDate));
                    } catch (ParseException e) {
                        logger.warn("Impossible to process interpretation status date {}: {}", status.getDate(), e.getMessage());
                    }
                }
            }

            if (StringUtils.isNotEmpty(interpretation.getCreationDate())) {
                try {
                    String solrDate = solrDateFormat.format(simpleDateFormat.parse(interpretation.getCreationDate()));
                    clinicalInterpretationSearch.setCreationDate(solrDateFormat.parse(solrDate));
                } catch (ParseException e) {
                    logger.warn("Impossible to process interpretation creation date {}: {}", interpretation.getCreationDate(), e.getMessage());
                }
            }

            if (StringUtils.isNotEmpty(interpretation.getModificationDate())) {
                try {
                    String solrDate = solrDateFormat.format(simpleDateFormat.parse(interpretation.getModificationDate()));
                    clinicalInterpretationSearch.setModificationDate(solrDateFormat.parse(solrDate));
                } catch (ParseException e) {
                    logger.warn("Impossible to process interpretation modification date {}: {}", interpretation.getModificationDate(),
                            e.getMessage());
                }
            }

            clinicalInterpretationSearch.setVersion(interpretation.getVersion());

            // Interpretation stored in a JSON string
            try {
                String json = mapper.writeValueAsString(interpretation);
                clinicalInterpretationSearch.setJson(ConverterUtils.compressToBase64(json));
            } catch (IOException e) {
                throw new CvdbException("Error when storing clinical interpretation JSON field", e);
            }

            // Add the new interpretation search model to the list
            clinicalInterpretationSearchList.add(clinicalInterpretationSearch);
        }
        return clinicalInterpretationSearchList;
    }

    public org.opencb.opencga.core.models.clinical.Interpretation toInterpretation(
            ClinicalInterpretationSearch clinicalInterpretationSearch) throws CvdbException {
        try {
            return interpretationReader.readValue(decompressFromBase64(clinicalInterpretationSearch.getJson()));
        } catch (IOException e) {
            throw new CvdbException("Error when converting to interpretation", e);
        }
    }

    @Override
    public org.opencb.opencga.core.models.clinical.Interpretation toModel(ClinicalInterpretationSearch input) throws CvdbException {
        return toInterpretation(input);
    }
}

