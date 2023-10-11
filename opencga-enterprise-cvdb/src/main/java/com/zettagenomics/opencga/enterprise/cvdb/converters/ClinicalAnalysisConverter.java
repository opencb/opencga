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
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalAnalysisSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.cvdb.converters.ConverterUtils.decompressFromBase64;

public class ClinicalAnalysisConverter extends SearchConverter {

    private ObjectReader clinicalAnalysisReader;

    private Logger logger;

    public ClinicalAnalysisConverter() {
        this.clinicalAnalysisReader = mapper.readerFor(ClinicalAnalysis.class);
        this.logger = LoggerFactory.getLogger(ClinicalAnalysisConverter.class);
    }

    public ClinicalAnalysisSearch toClinicalAnalysisSearch(ClinicalAnalysis clinicalAnalysis) throws CvdbException {
        return toClinicalAnalysisSearch(Collections.singletonList(clinicalAnalysis)).get(0);
    }

    public List<ClinicalAnalysisSearch> toClinicalAnalysisSearch(List<ClinicalAnalysis> clinicalAnalysisList) throws CvdbException {
        List<ClinicalAnalysisSearch> clinicalAnalysisSearchList = new ArrayList<>();

        for (ClinicalAnalysis ca : clinicalAnalysisList) {
            ClinicalAnalysisSearch cas = new ClinicalAnalysisSearch()
                    .setId(ca.getId())
                    .setDescription(ca.getDescription());

            if (ca.getType() != null) {
                cas.setType(ca.getType().name());
            }

            if (ca.getDisorder() != null) {
                cas.setDisorderId(ca.getDisorder().getId());
            }

            if (CollectionUtils.isNotEmpty(ca.getFiles())) {
                cas.setFileNames(ca.getFiles().stream().map(f -> f.getName()).collect(Collectors.toList()));
            }

            if (ca.getProband() != null) {
                cas.setProbandId(ca.getProband().getId());
            }

            if (ca.getFamily() != null) {
                cas.setFamilyId(ca.getFamily().getId());
                if (CollectionUtils.isNotEmpty(ca.getFamily().getPhenotypes())) {
                    cas.setFamilyPhenotypeNames(ca.getFamily().getPhenotypes().stream().map(p -> p.getId()).collect(Collectors.toList()));
                    cas.getFamilyPhenotypeNames().addAll(ca.getFamily().getPhenotypes().stream().map(p -> p.getName())
                            .collect(Collectors.toList()));
                }
                if (CollectionUtils.isNotEmpty(ca.getFamily().getMembers())) {
                    cas.setFamilyMemberIds(ca.getFamily().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList()));
                }
            }

            if (ca.getReport() != null && ca.getReport().getDiscussion() != null) {
                cas.setReport(ca.getReport().getDiscussion().getText());
            }

            if (ca.getStatus() != null) {
                cas.setStatus(ca.getStatus().getId());
            }

            cas.setLocked(ca.isLocked());

            try {
                String json = mapper.writeValueAsString(ca);
                cas.setJson(ConverterUtils.compressToBase64(json));
            } catch (IOException e) {
                throw new CvdbException("Error when storing clinical analysis JSON field", e);
            }

            // Add the new clinical analysis search model to the list
            clinicalAnalysisSearchList.add(cas);
        }
        return clinicalAnalysisSearchList;
    }

    public ClinicalAnalysis toClinicalAnalysis(ClinicalAnalysisSearch clinicalAnalysisSearch) throws CvdbException {
        try {
            return clinicalAnalysisReader.readValue(decompressFromBase64(clinicalAnalysisSearch.getJson()));
        } catch (IOException e) {
            throw new CvdbException("Error when converting to clinical analysis", e);
        }
    }
}

