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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.StringUtil;
import org.opencb.biodata.formats.pubmed.v233jaxb.I;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.InterpretationFindingStats;
import org.opencb.biodata.models.clinical.interpretation.InterpretationStats;
import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalReport;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.internal.util.LinkedArrayList;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.zettagenomics.opencga.enterprise.cvdb.converters.ConverterUtils.decompressFromBase64;

public class ClinicalAnalysisConverter extends SearchConverter<ClinicalAnalysis, ClinicalAnalysisSearch> {

    public static final String NUM_VARIANTS = "NV";
    public static final String GENE_COUNT = "GC";
    public static final String TIER_COUNT = "TC";
    public static final String STATUS_COUNT = "SC";

    private ObjectReader clinicalAnalysisReader;

    private static Logger logger = LoggerFactory.getLogger(ClinicalAnalysisConverter.class);

    public ClinicalAnalysisConverter() {
        this.clinicalAnalysisReader = mapper.readerFor(ClinicalAnalysis.class);
        this.logger = LoggerFactory.getLogger(ClinicalAnalysisConverter.class);
    }

    public ClinicalAnalysisSearch toClinicalAnalysisSearch(ClinicalAnalysis clinicalAnalysis, String studyId, List<String> users)
            throws CvdbException {
        return toClinicalAnalysisSearch(Collections.singletonList(clinicalAnalysis), studyId, users).get(0);
    }

    public List<ClinicalAnalysisSearch> toClinicalAnalysisSearch(List<ClinicalAnalysis> clinicalAnalysisList, String studyId,
                                                                 List<String> viewers) throws CvdbException {
        List<ClinicalAnalysisSearch> clinicalAnalysisSearchList = new ArrayList<>();

        for (ClinicalAnalysis ca : clinicalAnalysisList) {
            ClinicalAnalysisSearch cas = new ClinicalAnalysisSearch()
                    .setId(ca.getId())
                    .setDescription(ca.getDescription())
                    .setStudyId(studyId)
                    .setViewers(viewers);

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

            if (CollectionUtils.isNotEmpty(ca.getPanels())) {
                List<String> panelIds = ca.getPanels().stream().map(Panel::getId).collect(Collectors.toList());
                cas.setPanelIds(panelIds);
            }

            if (ca.getReport() != null && ca.getReport().getDiscussion() != null) {
                cas.setReport(ca.getReport().getDiscussion().getText());
            }

            if (ca.getStatus() != null) {
                cas.setStatus(ca.getStatus().getId());
            }

            cas.setLocked(ca.isLocked());

            try {
                // Full JSON
                String json = mapper.writeValueAsString(ca);
                cas.setFullJson(json);

                // Lite JSON
                if (CollectionUtils.isNotEmpty(ca.getPanels())) {
                    litePanels(ca.getPanels());
                }
                if (ca.getInterpretation() != null && CollectionUtils.isNotEmpty(ca.getInterpretation().getPanels())) {
                    litePanels(ca.getInterpretation().getPanels());
                }
                if (CollectionUtils.isNotEmpty(ca.getSecondaryInterpretations())) {
                    for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                        if (CollectionUtils.isNotEmpty(secondaryInterpretation.getPanels())) {
                            litePanels(secondaryInterpretation.getPanels());
                        }
                    }
                }
                json = mapper.writeValueAsString(ca);
                cas.setLiteJson(json);
            } catch (IOException e) {
                throw new CvdbException("Error when storing clinical analysis JSON field", e);
            }

            // Add the new clinical analysis search model to the list
            clinicalAnalysisSearchList.add(cas);
        }
        return clinicalAnalysisSearchList;
    }

    private void litePanels(List<Panel> panels) {
        for (Panel panel : panels) {
            panel.setVariants(null);
            panel.setStrs(null);
            panel.setGenes(null);
            panel.setRegions(null);
        }
    }

    public ClinicalAnalysis toClinicalAnalysis(ClinicalAnalysisSearch cas) throws CvdbException {
        ClinicalAnalysis ca;
        if (StringUtils.isNotEmpty(cas.getFullJson())) {
            // Build clinical analysis from the field 'fullJson'
            try {
                ca = clinicalAnalysisReader.readValue(cas.getFullJson());
            } catch (IOException e) {
                throw new CvdbException("Error when converting to clinical analysis from the field fullJson", e);
            }
        } else if (StringUtils.isNotEmpty(cas.getLiteJson())) {
            // Build clinical analysis from the field 'liteJson'
            try {
                ca = clinicalAnalysisReader.readValue(cas.getLiteJson());
            } catch (IOException e) {
                throw new CvdbException("Error when converting to clinical analysis from the field liteJson", e);
            }
        }  else {
            // Build clinical analysis from other fields
            ca = new ClinicalAnalysis();
            ca.setId(cas.getId());
            ca.setDescription(cas.getDescription());
            if (StringUtils.isNotEmpty(cas.getType())) {
                ca.setType(ClinicalAnalysis.Type.valueOf(cas.getType()));
            }
            if (StringUtils.isNotEmpty(cas.getDisorderId())) {
                ca.setDisorder(new Disorder().setId(cas.getDisorderId()));
            }
            if (CollectionUtils.isNotEmpty(cas.getFileNames())) {
                List<File> files = new ArrayList<>();
                for (String fileName : cas.getFileNames()) {
                    files.add(new File().setName(fileName));
                }
                ca.setFiles(files);
            }
            if (StringUtils.isNotEmpty(cas.getProbandId())) {
                ca.setProband(new Individual().setId(cas.getProbandId()));
            }
            if (StringUtils.isNotEmpty(cas.getFamilyId()) || CollectionUtils.isNotEmpty(cas.getFamilyPhenotypeNames())
                    || CollectionUtils.isNotEmpty(cas.getFamilyMemberIds())) {
                Family family = new Family().setId(cas.getFamilyId());
                if (CollectionUtils.isNotEmpty(cas.getFamilyPhenotypeNames())) {
                    List<Phenotype> phenotypes = new ArrayList<>();
                    for (String familyPhenotypeName : cas.getFamilyPhenotypeNames()) {
                        phenotypes.add(new Phenotype().setName(familyPhenotypeName));
                    }
                    family.setPhenotypes(phenotypes);
                }
                if (CollectionUtils.isNotEmpty(cas.getFamilyMemberIds())) {
                    List<Individual> members = new ArrayList<>();
                    for (String familyMemberId : cas.getFamilyMemberIds()) {
                        members.add(new Individual().setId(familyMemberId));
                    }
                    family.setMembers(members);
                }
                ca.setFamily(family);
            }
            if (CollectionUtils.isNotEmpty(cas.getPanelIds())) {
                List<Panel> panels = new ArrayList<>();
                for (String panelId : cas.getPanelIds()) {
                    panels.add(new Panel().setId(panelId));
                }
                ca.setPanels(panels);
            }
            if (StringUtils.isNotEmpty(cas.getReport())) {
                ca.setReport(new ClinicalReport().setDiscussion(new ClinicalDiscussion().setText(cas.getReport())));
            }
            if (StringUtils.isNotEmpty(cas.getStatus())) {
                ca.setStatus(new Status().setId(cas.getStatus()));
            }
            ca.setLocked(cas.isLocked());
        }

        return ca;
    }

    @Override
    public ClinicalAnalysis toModel(ClinicalAnalysisSearch input) throws CvdbException {
        return toClinicalAnalysis(input);
    }
}

