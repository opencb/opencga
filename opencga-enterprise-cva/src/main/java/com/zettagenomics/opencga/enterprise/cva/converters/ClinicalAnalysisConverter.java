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

package com.zettagenomics.opencga.enterprise.cva.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.zettagenomics.opencga.enterprise.cva.exceptions.CvaException;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalAnalysisSearch;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalInterpretationSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalAnalysisConverter extends SearchConverter {

    private ObjectReader clinicalAnalysisReader;

//    protected static Logger logger = LoggerFactory.getLogger(ClinicalAnalysisConverter.class);

    public ClinicalAnalysisConverter() {
        this.clinicalAnalysisReader = mapper.readerFor(ClinicalAnalysis.class);
    }

    public ClinicalAnalysisSearch toClinicalAnalysisSearch(ClinicalAnalysis clinicalAnalysis) throws CvaException {
        return toClinicalAnalysisSearch(Collections.singletonList(clinicalAnalysis)).get(0);
    }

    public List<ClinicalAnalysisSearch> toClinicalAnalysisSearch(List<ClinicalAnalysis> clinicalAnalysisList) throws CvaException {
        List<ClinicalAnalysisSearch> clinicalAnalysisSearchList = new ArrayList<>();
//        for (Interpretation interpretation : interpretations) {
//            ClinicalInterpretationSearch clinicalInterpretationSearch = new ClinicalInterpretationSearch()
//                    .setId(interpretation.getId())
//                    .setCaId(clinicalAnalsyisId)
//                    .setDescription(interpretation.getDescription())
//                    .setCaId(interpretation.getClinicalAnalysisId());
//
//            // Panels
//            if (CollectionUtils.isNotEmpty(interpretation.getPanels())) {
//                // Add panel IDs and names
//                clinicalInterpretationSearch.setPanelIds(interpretation.getPanels().stream().map(p -> p.getId())
//                        .collect(Collectors.toList()));
//                clinicalInterpretationSearch.getPanelIds().addAll(interpretation.getPanels().stream().map(p -> p.getName())
//                        .collect(Collectors.toList()));
//            }
//
//            // Analyst
//            if (interpretation.getAnalyst() != null) {
//                ClinicalAnalyst analyst = interpretation.getAnalyst();
//                clinicalInterpretationSearch.setAnalystId(analyst.getId())
//                        .setAnalystName(analyst.getName())
//                        .setAnalystEmail(analyst.getEmail())
//                        .setAnalystAssignedBy(analyst.getAssignedBy())
//                        .setAnalystDate(analyst.getDate());
//            }
//
//            // Method
//            if (interpretation.getMethod() != null) {
//                InterpretationMethod method = interpretation.getMethod();
//                clinicalInterpretationSearch.setMethodName(method.getName())
//                        .setMethodCommit(method.getCommit())
//                        .setMethodVersion(method.getVersion());
//                if (CollectionUtils.isNotEmpty(method.getDependencies())) {
//                    clinicalInterpretationSearch.setMethodDependencies(method.getDependencies().stream()
//                            .map(dp -> dp.getName() + ConverterUtils.FIELD_SEPARATOR + dp.getVersion())
//                            .collect(Collectors.toList()));
//                }
//            }
//
//            // Comments are stores: author -- message -- tag1:tag2:.. -- date -- -->
//            if (CollectionUtils.isNotEmpty(interpretation.getComments())) {
//                clinicalInterpretationSearch.setComments(interpretation.getComments().stream().map(c -> ConverterUtils.encodeComent(c))
//                        .collect(Collectors.toList()));
//            }
//
//            clinicalInterpretationSearch.setLocked(interpretation.isLocked());
//
//            // Status
//            if (interpretation.getStatus() != null) {
//                Status status = interpretation.getStatus();
//                clinicalInterpretationSearch.setStatusId(status.getId())
//                        .setStatusName(status.getName())
//                        .setStatusDescription(status.getDescription())
//                        .setStatusDate(status.getDate());
//            }
//
//            clinicalInterpretationSearch.setCreationDate(interpretation.getCreationDate());
//            clinicalInterpretationSearch.setModificationDate(interpretation.getModificationDate());
//
//            clinicalInterpretationSearch.setVersion(interpretation.getVersion());
//
//            // Interpretation stored in a JSON string
//            // First, we have to clone and remove the clinical variants to do not store them!
//            Interpretation clone = new Interpretation ();
//            clone.setPrimaryFindings(interpretation.getPrimaryFindings());
//            clone.setSecondaryFindings(interpretation.getSecondaryFindings());
//
//            interpretation.setPrimaryFindings(null);
//            interpretation.setSecondaryFindings(null);
//            try {
//                clinicalInterpretationSearch.setJson(mapper.writeValueAsString(interpretation));
//            } catch (JsonProcessingException e) {
//                throw new CvaException("Error when storing interpretation JSON field", e);
//            }
//            interpretation.setPrimaryFindings(clone.getPrimaryFindings());
//            interpretation.setSecondaryFindings(clone.getSecondaryFindings());
//
//            // Add the new interpretation search model to the list
//            clinicalInterpretationSearchList.add(clinicalInterpretationSearch);
//        }
        return clinicalAnalysisSearchList;
    }

    public ClinicalAnalysis toClinicalAnalysis(ClinicalAnalysisSearch clinicalAnalysisSearch) throws CvaException {
        try {
            return clinicalAnalysisReader.readValue(clinicalAnalysisSearch.getJson());
        } catch (JsonProcessingException e) {
            throw new CvaException("Error when converting to clinical analysis", e);
        }
    }
}

