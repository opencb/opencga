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

import org.apache.avro.generic.GenericRecord;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisConverter extends OpenCgaMongoConverter<ClinicalAnalysis> {

    public ClinicalAnalysisConverter() {
        super(ClinicalAnalysis.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
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
    }

    public void validateInterpretationToUpdate(Document document) {
        Document interpretation = (Document) document.get(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key());
        if (interpretation != null) {
            document.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(),
                    new Document(InterpretationDBAdaptor.QueryParams.UID.key(),
                            getLongValue(interpretation, InterpretationDBAdaptor.QueryParams.UID.key())));
        }
    }

    public void validateSecondaryInterpretationsToUpdate(Document document) {
        List<Document> interpretationList = (List) document.get(ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key());
        if (interpretationList != null) {
            List<Document> newInterpretationList = new ArrayList<>();
            for (int i = 0; i < interpretationList.size(); i++) {
                newInterpretationList.add(new Document(InterpretationDBAdaptor.QueryParams.UID.key(),
                        getLongValue(interpretationList.get(i), InterpretationDBAdaptor.QueryParams.UID.key())));
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
            long uid = getLongValue(sample, SampleDBAdaptor.QueryParams.UID.key());
            if (uid > 0) {
                Sample tmpSample = new Sample()
                        .setUid(uid)
                        .setId(sample.getString(SampleDBAdaptor.QueryParams.ID.key()))
                        .setVersion(sample.getInteger(SampleDBAdaptor.QueryParams.VERSION.key()));
                sampleMap.put(uid, tmpSample);
            }
        }

        document.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(),
                sampleMap.entrySet().stream()
                        .map(entry -> new Document()
                                .append(SampleDBAdaptor.QueryParams.ID.key(), entry.getValue().getId())
                                .append(SampleDBAdaptor.QueryParams.UID.key(), entry.getValue().getUid())
                                .append(SampleDBAdaptor.QueryParams.VERSION.key(), entry.getValue().getVersion()))
                        .collect(Collectors.toList()));
    }

    public void validateProbandToUpdate(Document document) {
        Document proband = (Document) document.get(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key());
        if (proband == null) {
            return;
        }

        validateSamplesToUpdate(proband);

        document.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), new Document()
                .append(IndividualDBAdaptor.QueryParams.UID.key(), getLongValue(proband, IndividualDBAdaptor.QueryParams.UID.key()))
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
                        .append(IndividualDBAdaptor.QueryParams.UID.key(), getLongValue(entry, IndividualDBAdaptor.QueryParams.UID.key()))
                        .append(IndividualDBAdaptor.QueryParams.ID.key(), entry.getString(IndividualDBAdaptor.QueryParams.ID.key()))
                        .append(IndividualDBAdaptor.QueryParams.VERSION.key(),
                                entry.getInteger(IndividualDBAdaptor.QueryParams.VERSION.key()))
                        .append(IndividualDBAdaptor.QueryParams.SAMPLES.key(), entry.get(IndividualDBAdaptor.QueryParams.SAMPLES.key()))
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
                .append(FamilyDBAdaptor.QueryParams.UID.key(), getLongValue(family, FamilyDBAdaptor.QueryParams.UID.key()))
                .append(FamilyDBAdaptor.QueryParams.ID.key(), family.get(FamilyDBAdaptor.QueryParams.ID.key()))
                .append(FamilyDBAdaptor.QueryParams.VERSION.key(), family.get(FamilyDBAdaptor.QueryParams.VERSION.key()))
                .append(FamilyDBAdaptor.QueryParams.MEMBERS.key(), family.get(FamilyDBAdaptor.QueryParams.MEMBERS.key()))
        );
    }

}
