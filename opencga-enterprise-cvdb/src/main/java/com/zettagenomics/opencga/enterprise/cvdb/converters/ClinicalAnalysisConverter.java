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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalReport;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ClinicalAnalysisConverter extends SearchConverter<ClinicalAnalysis, ClinicalAnalysisSearch> {

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
                // Maximum JSON
                String json = mapper.writeValueAsString(ca);
                cas.setMaxJson(json);

                // Clinical analysis copy
                ClinicalAnalysis copy = clinicalAnalysisReader.readValue(cas.getMaxJson());

                // Medium JSON
                //  - removing panels
                if (CollectionUtils.isNotEmpty(copy.getPanels())) {
                    minimizePanels(copy.getPanels());
                }
                //  - removing interpretation.panels
                if (copy.getInterpretation() != null && CollectionUtils.isNotEmpty(copy.getInterpretation().getPanels())) {
                    minimizePanels(copy.getInterpretation().getPanels());
                }
                //  - removing secondaryInterpretations.panels
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
                //  - from disorder, removing: attributes and evidences
                //    and keeping: id, name, description, source, url
                if (copy.getDisorder() != null) {
                    copy.getDisorder().setAttributes(null);
                    copy.getDisorder().setEvidences(null);
                }
                //  - from files, removing all except id and name
                if (CollectionUtils.isNotEmpty(copy.getFiles())) {
                    List<File> newFiles = new ArrayList<>();
                    for (File file : copy.getFiles()) {
                        newFiles.add(new File().setId(file.getId()).setName(file.getName()));
                    }
                }
                //  - from proband, removing all except id, sex and samples.id
                if (copy.getProband() != null) {
                    copy.setProband(getMinimizedIndividual(copy.getProband()));
                }

                //  - from family, removing all except id, name and members.id, members.sex and members.samples.id
                if (copy.getFamily() != null) {
                    copy.setFamily(getMinimizedFamily(copy.getFamily()));
                }

                //  - from panels, removing except id, name, source and stats
                if (CollectionUtils.isNotEmpty(copy.getPanels())) {
                    List<Panel> newPanels = copy.getPanels().stream().map(p -> getMinimizedPanel(p)).collect(Collectors.toList());
                    copy.setPanels(newPanels);
                }

                //  - from interpretation, remove all except method and stats
                if (copy.getInterpretation() != null) {
                    Interpretation newInterpretation = new Interpretation();
                    newInterpretation.setId(copy.getInterpretation().getId());
                    newInterpretation.setMethod(copy.getInterpretation().getMethod());
                    newInterpretation.setStats(copy.getInterpretation().getStats());
                    copy.setInterpretation(newInterpretation);
                }

                //  - from secondaryInterpretations, remove all
                copy.setSecondaryInterpretations(null);

                json = mapper.writeValueAsString(copy);
                cas.setMinJson(json);


            } catch (IOException e) {
                throw new CvdbException("Error when storing clinical analysis JSON field", e);
            }

            // Add the new clinical analysis search model to the list
            clinicalAnalysisSearchList.add(cas);
        }
        return clinicalAnalysisSearchList;
    }

    private void minimizePanels(List<Panel> panels) {
        for (Panel panel : panels) {
            panel.setVariants(null);
            panel.setStrs(null);
            panel.setGenes(null);
            panel.setRegions(null);
        }
    }

    private Individual getMinimizedIndividual(Individual oldIndividual) {
        Individual newIndividual = new Individual()
                .setId(oldIndividual.getId())
                .setName(oldIndividual.getName())
                .setSex(oldIndividual.getSex());
        if (CollectionUtils.isNotEmpty(oldIndividual.getSamples())) {
            List<Sample> newSamples = oldIndividual.getSamples().stream().map(s -> new Sample().setId(s.getId()))
                    .collect(Collectors.toList());
            newIndividual.setSamples(newSamples);
        }
        return newIndividual;
    }

    private Family getMinimizedFamily(Family oldFamily) {
        Family newFamily = new Family()
                .setId(oldFamily.getId())
                .setName(oldFamily.getName());
        if (CollectionUtils.isNotEmpty(oldFamily.getMembers())) {
            List<Individual> newMembers = oldFamily.getMembers().stream().map(m -> getMinimizedIndividual(m)).collect(Collectors.toList());
            newFamily.setMembers(newMembers);
        }
        return newFamily;
    }

    private Panel getMinimizedPanel(Panel oldPanel) {
        Panel newPanel = new Panel();
        newPanel.setId(oldPanel.getId());
        newPanel.setName(oldPanel.getName());
        newPanel.setSource(oldPanel.getSource());
        newPanel.setStats(oldPanel.getStats());
        return newPanel;
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

