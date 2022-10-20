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

package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisConverter extends OpenCgaMongoConverter<ClinicalAnalysis> {

    public ClinicalAnalysisConverter() {
        super(ClinicalAnalysis.class);
    }

    @Override
    public Document convertToStorageType(ClinicalAnalysis clinicalAnalysis) {
        Document document = super.convertToStorageType(clinicalAnalysis);
        document.put(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), clinicalAnalysis.getUid());
        document.put(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), clinicalAnalysis.getStudyUid());

        validateDocumentToUpdate(document);

        return document;
    }

    public void validateDocumentToUpdate(Document document) {
        validateInterpretationToUpdate(document);
        validateSecondaryInterpretationsToUpdate(document);
        validateFamilyToUpdate(document);
        validateProbandToUpdate(document);
        validatePanelsToUpdate(document);
        validateFilesToUpdate(document);
        validateReportToUpdate(document);
    }

    public void validateInterpretationToUpdate(Document document) {
        Document interpretation = (Document) document.get(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key());
        if (interpretation != null) {
            document.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(),
                    new Document()
                            .append(InterpretationDBAdaptor.QueryParams.UID.key(),
                                    ((Number) interpretation.get(InterpretationDBAdaptor.QueryParams.UID.key())).longValue())
                            .append(InterpretationDBAdaptor.QueryParams.VERSION.key(),
                                    interpretation.getInteger(InterpretationDBAdaptor.QueryParams.VERSION.key())));
        }
    }

    public void validateSecondaryInterpretationsToUpdate(Document document) {
        List<Document> interpretationList = (List) document.get(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key());
        if (interpretationList != null) {
            List<Document> newInterpretationList = new ArrayList<>();
            for (int i = 0; i < interpretationList.size(); i++) {
                newInterpretationList.add(new Document()
                        .append(InterpretationDBAdaptor.QueryParams.UID.key(),
                                ((Number) interpretationList.get(i).get(InterpretationDBAdaptor.QueryParams.UID.key())).longValue())
                        .append(InterpretationDBAdaptor.QueryParams.VERSION.key(),
                                interpretationList.get(i).getInteger(InterpretationDBAdaptor.QueryParams.VERSION.key())));
            }

            document.put(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), newInterpretationList);
        }
    }

    public void validateSamplesToUpdate(Document document) {
        List<Document> samples = (List) document.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
        if (samples == null) {
            return;
        }

        // We make sure we don't store duplicates
        Map<Long, Sample> sampleMap = new HashMap<>();
        for (Document sample : samples) {
            long uid = ((Number) sample.get(SampleDBAdaptor.QueryParams.UID.key())).longValue();
            if (uid > 0) {
                Sample tmpSample = new Sample()
                        .setUid(uid)
                        .setId(sample.getString(SampleDBAdaptor.QueryParams.ID.key()));
                sampleMap.put(uid, tmpSample);
            }
        }

        document.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                sampleMap.entrySet().stream()
                        .map(entry -> new Document()
                                .append(SampleDBAdaptor.QueryParams.ID.key(), entry.getValue().getId())
                                .append(SampleDBAdaptor.QueryParams.UID.key(), entry.getValue().getUid()))
                        .collect(Collectors.toList()));
    }

    public void validatePanelsToUpdate(Document document) {
        List<Document> panels = (List) document.get(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key());
        if (panels != null) {
            // We make sure we don't store duplicates
            Map<Long, Panel> panelMap = new HashMap<>();
            for (Document panel : panels) {
                long uid = panel.get(PanelDBAdaptor.QueryParams.UID.key(), Number.class).longValue();
                int version = panel.getInteger(PanelDBAdaptor.QueryParams.VERSION.key());
                if (uid > 0) {
                    Panel tmpPanel = new Panel()
                            .setId(panel.getString(PanelDBAdaptor.QueryParams.ID.key()))
                            .setVersion(version);
                    tmpPanel.setUid(uid);
                    panelMap.put(uid, tmpPanel);
                }
            }

            document.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(),
                    panelMap.entrySet().stream()
                            .map(entry -> new Document()
                                    .append(PanelDBAdaptor.QueryParams.ID.key(), entry.getValue().getId())
                                    .append(PanelDBAdaptor.QueryParams.UID.key(), entry.getValue().getUid())
                                    .append(PanelDBAdaptor.QueryParams.VERSION.key(), entry.getValue().getVersion()))
                            .collect(Collectors.toList()));
        }
    }

    public void validateFilesToUpdate(Document document) {
        List<Document> files = (List) document.get(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key());
        if (files != null) {
            List<File> uniqueListOfFiles = getUniqueListOfFiles(files);

            document.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(),
                    uniqueListOfFiles.stream()
                            .map(file -> new Document()
                                    .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath())
                                    .append(FileDBAdaptor.QueryParams.UID.key(), file.getUid()))
                            .collect(Collectors.toList()));
        }
    }

    private List<File> getUniqueListOfFiles(List<Document> files) {
        if (files != null) {
            // We make sure we don't store duplicates
            Map<Long, File> fileMap = new HashMap<>();
            for (Document file : files) {
                long uid = file.get(FileDBAdaptor.QueryParams.UID.key(), Number.class).longValue();
                if (uid > 0) {
                    File tmpFile = new File()
                            .setPath(file.getString(FileDBAdaptor.QueryParams.PATH.key()))
                            .setUid(uid);
                    fileMap.put(uid, tmpFile);
                }
            }

            return new ArrayList<>(fileMap.values());
        } else {
            return Collections.emptyList();
        }
    }

    public void validateProbandToUpdate(Document document) {
        Document proband = (Document) document.get(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key());
        if (proband == null) {
            return;
        }

        validateSamplesToUpdate(proband);

        document.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), new Document()
                .append(IndividualDBAdaptor.QueryParams.UID.key(),
                        ((Number) proband.get(IndividualDBAdaptor.QueryParams.UID.key())).longValue())
                .append(IndividualDBAdaptor.QueryParams.ID.key(), proband.getString(IndividualDBAdaptor.QueryParams.ID.key()))
                .append(IndividualDBAdaptor.QueryParams.VERSION.key(),
                        proband.getInteger(IndividualDBAdaptor.QueryParams.VERSION.key()))
                .append(IndividualDBAdaptor.QueryParams.SAMPLES.key(), proband.get(IndividualDBAdaptor.QueryParams.SAMPLES.key()))
        );
    }

    public void validateFamilyMembersToUpdate(Document document) {
        List<Document> members = (List<Document>) document.get(FamilyDBAdaptor.QueryParams.MEMBERS.key());
        if (members == null) {
            return;
        }

        for (Document member : members) {
            validateSamplesToUpdate(member);
        }

        document.put(FamilyDBAdaptor.QueryParams.MEMBERS.key(),
                members.stream().map(entry -> new Document()
                                .append(IndividualDBAdaptor.QueryParams.UID.key(),
                                        ((Number) entry.get(IndividualDBAdaptor.QueryParams.UID.key())).longValue())
                                .append(IndividualDBAdaptor.QueryParams.ID.key(),
                                        entry.getString(IndividualDBAdaptor.QueryParams.ID.key()))
                                .append(IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                                        entry.get(IndividualDBAdaptor.QueryParams.SAMPLES.key()))
                        )
                        .collect(Collectors.toList()));
    }

    public void validateFamilyToUpdate(Document document) {
        Document family = (Document) document.get(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key());
        if (family == null) {
            return;
        }

        validateFamilyMembersToUpdate(family);

        document.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), new Document()
                .append(FamilyDBAdaptor.QueryParams.UID.key(), ((Number) family.get(FamilyDBAdaptor.QueryParams.UID.key())).longValue())
                .append(FamilyDBAdaptor.QueryParams.ID.key(), family.get(FamilyDBAdaptor.QueryParams.ID.key()))
                .append(FamilyDBAdaptor.QueryParams.VERSION.key(), family.getInteger(FamilyDBAdaptor.QueryParams.VERSION.key()))
                .append(FamilyDBAdaptor.QueryParams.MEMBERS.key(), family.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()))
        );
    }

    public void validateReportToUpdate(Document document) {
        Document report = document.get(ClinicalAnalysisDBAdaptor.QueryParams.REPORT.key(), Document.class);
        if (report != null) {
            String annexesKey = ClinicalAnalysisDBAdaptor.QueryParams.REPORT_ANNEXES.key()
                    .replace(ClinicalAnalysisDBAdaptor.QueryParams.REPORT.key() + ".", "");
            List<Document> files = report.getList(annexesKey, Document.class);

            List<File> uniqueListOfFiles = getUniqueListOfFiles(files);

            report.put(annexesKey, uniqueListOfFiles.stream()
                    .map(file -> new Document()
                            .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath())
                            .append(FileDBAdaptor.QueryParams.UID.key(), file.getUid()))
                    .collect(Collectors.toList()));
        }
    }

}
