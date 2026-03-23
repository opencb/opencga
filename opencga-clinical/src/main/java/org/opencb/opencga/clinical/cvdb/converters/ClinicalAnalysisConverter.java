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

package org.opencb.opencga.clinical.cvdb.converters;

import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.opencga.clinical.cvdb.exceptions.CvdbException;
import org.opencb.opencga.clinical.cvdb.models.ClinicalAnalysisSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalAnalysisConverter extends SearchConverter<ClinicalAnalysis, ClinicalAnalysisSearch> {

    private ObjectReader clinicalAnalysisReader;

    public ClinicalAnalysisConverter() {
        this.clinicalAnalysisReader = mapper.readerFor(ClinicalAnalysis.class);
    }

    public ClinicalAnalysisSearch toClinicalAnalysisSearch(ClinicalAnalysis clinicalAnalysis, String studyId)
            throws CvdbException {
        return toClinicalAnalysisSearch(Collections.singletonList(clinicalAnalysis), studyId).get(0);
    }

    public List<ClinicalAnalysisSearch> toClinicalAnalysisSearch(List<ClinicalAnalysis> clinicalAnalysisList, String studyId)
            throws CvdbException {
        List<ClinicalAnalysisSearch> clinicalAnalysisSearchList = new ArrayList<>();

        for (ClinicalAnalysis ca : clinicalAnalysisList) {
            ClinicalAnalysisSearch cas = new ClinicalAnalysisSearch()
                    .setId(ca.getId())
                    .setDescription(ca.getDescription())
                    .setStudyId(studyId);

            if (ca.getType() != null) {
                cas.setType(ca.getType().name());
            }

            if (ca.getDisorder() != null) {
                cas.setDisorderId(ca.getDisorder().getId());
            }

            if (CollectionUtils.isNotEmpty(ca.getFiles())) {
                cas.setFileNames(ca.getFiles().stream().map(File::getName).collect(Collectors.toList()));
            }

            if (ca.getProband() != null) {
                cas.setProbandId(ca.getProband().getId());

                if (CollectionUtils.isNotEmpty(ca.getProband().getDisorders())) {
                    List<String> ids = ca.getProband().getDisorders().stream().map(Disorder::getId).collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(ids)) {
                        cas.setProbandDisorderIds(ids);
                    }
                }

                if (CollectionUtils.isNotEmpty(ca.getProband().getPhenotypes())) {
                    List<String> names = ca.getProband().getPhenotypes().stream().map(Phenotype::getName).collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(names)) {
                        cas.setProbandPhenotypeNames(names);
                    }
                }
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
                // Maximum JSON
                String json = mapper.writeValueAsString(ca);
                cas.setMaxJson(json);

                // Clinical analysis copy
                ClinicalAnalysis copy = clinicalAnalysisReader.readValue(json);

                // Medium JSON
                //  - minimizing panels
                if (CollectionUtils.isNotEmpty(copy.getPanels())) {
                    minimizePanels(copy.getPanels());
                }
                //  - minimizing interpretation.panels
                if (copy.getInterpretation() != null && CollectionUtils.isNotEmpty(copy.getInterpretation().getPanels())) {
                    minimizePanels(copy.getInterpretation().getPanels());
                }
                //  - minimizing secondaryInterpretations.panels
                if (CollectionUtils.isNotEmpty(copy.getSecondaryInterpretations())) {
                    for (Interpretation secondaryInterpretation : copy.getSecondaryInterpretations()) {
                        if (CollectionUtils.isNotEmpty(secondaryInterpretation.getPanels())) {
                            minimizePanels(secondaryInterpretation.getPanels());
                        }
                    }
                }
                json = mapper.writeValueAsString(copy);
                cas.setMediumJson(json);

                // Minimum JSON
                //  - from interpretation, minimize primary and secondary findings, i.e., remove variant annotation
                if (copy.getInterpretation() != null) {
                    minimizeClinicalVariants(copy.getInterpretation().getPrimaryFindings());
                    minimizeClinicalVariants(copy.getInterpretation().getSecondaryFindings());
                }

                //  - from secondaryInterpretations, minimize primary and secondary findings, i.e., remove variant annotation
                if (CollectionUtils.isNotEmpty(copy.getSecondaryInterpretations())) {
                    for (Interpretation secondaryInterpretation : copy.getSecondaryInterpretations()) {
                        minimizeClinicalVariants(secondaryInterpretation.getPrimaryFindings());
                        minimizeClinicalVariants(secondaryInterpretation.getSecondaryFindings());
                    }
                }

                json = mapper.writeValueAsString(copy);
                cas.setMinJson(json);
            } catch (IOException e) {
                throw new CvdbException("Error when storing clinical analysis JSON fields", e);
            }

            // Add the new clinical analysis search model to the list
            clinicalAnalysisSearchList.add(cas);
        }
        return clinicalAnalysisSearchList;
    }

    public ClinicalAnalysis toClinicalAnalysis(ClinicalAnalysisSearch cas) throws CvdbException {
        ClinicalAnalysis ca;
        if (StringUtils.isNotEmpty(cas.getMaxJson())) {
            // Build clinical analysis from the field 'maxJson'
            try {
                ca = clinicalAnalysisReader.readValue(cas.getMaxJson());
            } catch (IOException e) {
                throw new CvdbException("Error when converting to clinical analysis from the field maxJson", e);
            }
        } else if (StringUtils.isNotEmpty(cas.getMediumJson())) {
            // Build clinical analysis from the field 'mediumJson'
            try {
                ca = clinicalAnalysisReader.readValue(cas.getMediumJson());
            } catch (IOException e) {
                throw new CvdbException("Error when converting to clinical analysis from the field mediumJson", e);
            }
        } else if (StringUtils.isNotEmpty(cas.getMinJson())) {
            // Build clinical analysis from the field 'minJson'
            try {
                ca = clinicalAnalysisReader.readValue(cas.getMinJson());
            } catch (IOException e) {
                throw new CvdbException("Error when converting to clinical analysis from the field minJson", e);
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
            ca.setProband(new Individual());
            if (StringUtils.isNotEmpty(cas.getProbandId())) {
                ca.getProband().setId(cas.getProbandId());
            }
            if (CollectionUtils.isNotEmpty(cas.getProbandDisorderIds())) {
                List<Disorder> disorderList = new ArrayList<>();
                for (String id : cas.getProbandDisorderIds()) {
                    if (StringUtils.isNotEmpty(id)) {
                        disorderList.add(new Disorder().setId(id));
                    }
                }
                if (CollectionUtils.isNotEmpty(disorderList)) {
                    ca.getProband().setDisorders(disorderList);
                }
            }
            if (CollectionUtils.isNotEmpty(cas.getProbandPhenotypeNames())) {
                List<Phenotype> phenotypeList = new ArrayList<>();
                for (String name : cas.getProbandPhenotypeNames()) {
                    if (StringUtils.isNotEmpty(name)) {
                        phenotypeList.add(new Phenotype().setName(name));
                    }
                }
                if (CollectionUtils.isNotEmpty(phenotypeList)) {
                    ca.getProband().setPhenotypes(phenotypeList);
                }
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
                ClinicalStatus clinicalStatus = new ClinicalStatus();
                clinicalStatus.setType(ClinicalStatusValue.ClinicalStatusType.valueOf(cas.getStatus()));
                ca.setStatus(clinicalStatus);
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

