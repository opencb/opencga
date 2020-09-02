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

package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileReferenceParam;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;
import java.util.stream.Collectors;

public class ClinicalAnalysisCreateParams {

    private String id;
    private String description;
    private ClinicalAnalysis.Type type;

    private DisorderReferenceParam disorder;

    private List<FileReferenceParam> files;

    private ProbandParam proband;
    private FamilyParam family;
    private ClinicalAnalystParam analyst;
    private ClinicalAnalysisInternal internal;
    private InterpretationCreateParams interpretation;
    private List<InterpretationCreateParams> secondaryInterpretations;

    private ClinicalConsent consent;

    private String dueDate;
    private List<ClinicalComment> comments;
    private List<ClinicalAudit> audit;
    private Enums.Priority priority;
    private List<String> flags;

    private Map<String, Object> attributes;
    private CustomStatusParams status;

    public ClinicalAnalysisCreateParams() {
    }

    public ClinicalAnalysisCreateParams(String id, String description, ClinicalAnalysis.Type type, DisorderReferenceParam disorder,
                                        List<FileReferenceParam> files, ProbandParam proband, FamilyParam family,
                                        ClinicalAnalystParam analyst, ClinicalAnalysisInternal internal,
                                        InterpretationCreateParams interpretation,
                                        List<InterpretationCreateParams> secondaryInterpretations, ClinicalConsent consent, String dueDate,
                                        List<ClinicalComment> comments, List<ClinicalAudit> audit, Enums.Priority priority,
                                        List<String> flags, Map<String, Object> attributes, CustomStatusParams status) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.analyst = analyst;
        this.internal = internal;
        this.interpretation = interpretation;
        this.secondaryInterpretations = secondaryInterpretations;
        this.consent = consent;
        this.dueDate = dueDate;
        this.comments = comments;
        this.audit = audit;
        this.priority = priority;
        this.flags = flags;
        this.attributes = attributes;
        this.status = status;
    }

    public static ClinicalAnalysisCreateParams of(ClinicalAnalysis clinicalAnalysis) {
        return new ClinicalAnalysisCreateParams(clinicalAnalysis.getId(), clinicalAnalysis.getDescription(),
                clinicalAnalysis.getType(), DisorderReferenceParam.of(clinicalAnalysis.getDisorder()),
                clinicalAnalysis.getFiles() != null
                        ? clinicalAnalysis.getFiles().stream().map(FileReferenceParam::of).collect(Collectors.toList())
                        : null,
                clinicalAnalysis.getProband() != null ? ProbandParam.of(clinicalAnalysis.getProband()) : null,
                clinicalAnalysis.getFamily() != null ? FamilyParam.of(clinicalAnalysis.getFamily()) : null,
                clinicalAnalysis.getAnalyst() != null ? ClinicalAnalystParam.of(clinicalAnalysis.getAnalyst()) : null,
                clinicalAnalysis.getInternal(),
                clinicalAnalysis.getInterpretation() != null
                        ? InterpretationCreateParams.of(clinicalAnalysis.getInterpretation())
                        : null,
                clinicalAnalysis.getSecondaryInterpretations() != null
                        ? clinicalAnalysis.getSecondaryInterpretations().stream().map(InterpretationCreateParams::of)
                        .collect(Collectors.toList())
                        : Collections.emptyList(),
                clinicalAnalysis.getConsent(), clinicalAnalysis.getDueDate(),
                clinicalAnalysis.getComments(), clinicalAnalysis.getAudit(), clinicalAnalysis.getPriority(), clinicalAnalysis.getFlags(),
                clinicalAnalysis.getAttributes(), CustomStatusParams.of(clinicalAnalysis.getStatus()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", disorder=").append(disorder);
        sb.append(", files=").append(files);
        sb.append(", proband=").append(proband);
        sb.append(", family=").append(family);
        sb.append(", analyst=").append(analyst);
        sb.append(", internal=").append(internal);
        sb.append(", interpretation=").append(interpretation);
        sb.append(", secondaryInterpretations=").append(secondaryInterpretations);
        sb.append(", consent=").append(consent);
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", audit=").append(audit);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", attributes=").append(attributes);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public ClinicalAnalysis toClinicalAnalysis() {

        Individual individual = null;
        if (proband != null) {
            individual = new Individual().setId(proband.id);
            if (proband.samples != null) {
                List<Sample> sampleList = proband.samples.stream()
                        .map(sample -> new Sample().setId(sample.id))
                        .collect(Collectors.toList());
                individual.setSamples(sampleList);
            }
        }

        Family f = null;
        if (family != null) {
            f = new Family().setId(family.id);
            if (family.members != null) {
                List<Individual> members = new ArrayList<>(family.members.size());
                for (ProbandParam member : family.members) {
                    Individual auxIndividual = new Individual().setId(member.id);
                    if (member.samples != null) {
                        List<Sample> samples = member.samples.stream().map(s -> new Sample().setId(s.id)).collect(Collectors.toList());
                        auxIndividual.setSamples(samples);
                    }
                    members.add(auxIndividual);
                }
                f.setMembers(members);
            }
        }

        Interpretation primaryInterpretation = secondaryInterpretations != null ? interpretation.toClinicalInterpretation() : null;

        List<Interpretation> secondaryInterpretationList =
                secondaryInterpretations != null
                        ? secondaryInterpretations.stream()
                        .map(InterpretationCreateParams::toClinicalInterpretation)
                        .collect(Collectors.toList())
                        : new ArrayList<>();

        String assignee = analyst != null ? analyst.id : "";

        List<File> caFiles = new LinkedList<>();
        if (files != null) {
            for (FileReferenceParam file : files) {
                caFiles.add(file.toFile());
            }
        }

        return new ClinicalAnalysis(id, description, type, disorder != null ? disorder.toDisorder() : null, caFiles, individual, f,
                primaryInterpretation, secondaryInterpretationList, consent, new ClinicalAnalyst(assignee, assignee, "", "",
                TimeUtils.getTime()), priority, flags, null, null,  dueDate, 1, comments, audit, internal, attributes,
                status != null ? status.toCustomStatus() : null);
    }

    public String getId() {
        return id;
    }

    public ClinicalAnalysisCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalAnalysisCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalAnalysis.Type getType() {
        return type;
    }

    public ClinicalAnalysisCreateParams setType(ClinicalAnalysis.Type type) {
        this.type = type;
        return this;
    }

    public DisorderReferenceParam getDisorder() {
        return disorder;
    }

    public ClinicalAnalysisCreateParams setDisorder(DisorderReferenceParam disorder) {
        this.disorder = disorder;
        return this;
    }

    public List<FileReferenceParam> getFiles() {
        return files;
    }

    public ClinicalAnalysisCreateParams setFiles(List<FileReferenceParam> files) {
        this.files = files;
        return this;
    }

    public ProbandParam getProband() {
        return proband;
    }

    public ClinicalAnalysisCreateParams setProband(ProbandParam proband) {
        this.proband = proband;
        return this;
    }

    public FamilyParam getFamily() {
        return family;
    }

    public ClinicalAnalysisCreateParams setFamily(FamilyParam family) {
        this.family = family;
        return this;
    }

    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    public ClinicalAnalysisCreateParams setAnalyst(ClinicalAnalystParam analyst) {
        this.analyst = analyst;
        return this;
    }

    public ClinicalAnalysisInternal getInternal() {
        return internal;
    }

    public ClinicalAnalysisCreateParams setInternal(ClinicalAnalysisInternal internal) {
        this.internal = internal;
        return this;
    }

    public InterpretationCreateParams getInterpretation() {
        return interpretation;
    }

    public ClinicalAnalysisCreateParams setInterpretation(InterpretationCreateParams interpretation) {
        this.interpretation = interpretation;
        return this;
    }

    public List<InterpretationCreateParams> getSecondaryInterpretations() {
        return secondaryInterpretations;
    }

    public ClinicalAnalysisCreateParams setSecondaryInterpretations(List<InterpretationCreateParams> secondaryInterpretations) {
        this.secondaryInterpretations = secondaryInterpretations;
        return this;
    }

    public ClinicalConsent getConsent() {
        return consent;
    }

    public ClinicalAnalysisCreateParams setConsent(ClinicalConsent consent) {
        this.consent = consent;
        return this;
    }

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalAnalysisCreateParams setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public ClinicalAnalysisCreateParams setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }

    public List<ClinicalAudit> getAudit() {
        return audit;
    }

    public ClinicalAnalysisCreateParams setAudit(List<ClinicalAudit> audit) {
        this.audit = audit;
        return this;
    }

    public Enums.Priority getPriority() {
        return priority;
    }

    public ClinicalAnalysisCreateParams setPriority(Enums.Priority priority) {
        this.priority = priority;
        return this;
    }

    public List<String> getFlags() {
        return flags;
    }

    public ClinicalAnalysisCreateParams setFlags(List<String> flags) {
        this.flags = flags;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalAnalysisCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public ClinicalAnalysisCreateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    private static class SampleParams {
        private String id;

        public SampleParams() {
        }

        public SampleParams(String id) {
            this.id = id;
        }

        public static SampleParams of(Sample sample) {
            return new SampleParams(sample.getId());
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SampleParams{");
            sb.append("id='").append(id).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public SampleParams setId(String id) {
            this.id = id;
            return this;
        }
    }

    private static class ProbandParam {
        private String id;
        private List<SampleParams> samples;

        public ProbandParam() {
        }

        public ProbandParam(String id, List<SampleParams> samples) {
            this.id = id;
            this.samples = samples;
        }

        public static ProbandParam of(Individual individual) {
            return new ProbandParam(individual.getId(),
                    individual.getSamples() != null
                            ? individual.getSamples().stream().map(SampleParams::of).collect(Collectors.toList())
                            : Collections.emptyList());
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ProbandParam{");
            sb.append("id='").append(id).append('\'');
            sb.append(", samples=").append(samples);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public ProbandParam setId(String id) {
            this.id = id;
            return this;
        }

        public List<SampleParams> getSamples() {
            return samples;
        }

        public ProbandParam setSamples(List<SampleParams> samples) {
            this.samples = samples;
            return this;
        }
    }

    private static class FamilyParam {
        private String id;
        private List<ProbandParam> members;

        public FamilyParam() {
        }

        public FamilyParam(String id, List<ProbandParam> members) {
            this.id = id;
            this.members = members;
        }

        public static FamilyParam of(Family family) {
            return new FamilyParam(family.getId(),
                    family.getMembers() != null
                            ? family.getMembers().stream().map(ProbandParam::of).collect(Collectors.toList())
                            : Collections.emptyList());
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FamilyParam{");
            sb.append("id='").append(id).append('\'');
            sb.append(", members=").append(members);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public FamilyParam setId(String id) {
            this.id = id;
            return this;
        }

        public List<ProbandParam> getMembers() {
            return members;
        }

        public FamilyParam setMembers(List<ProbandParam> members) {
            this.members = members;
            return this;
        }
    }

    private static class ClinicalAnalystParam {
        private String id;

        public ClinicalAnalystParam() {
        }

        public ClinicalAnalystParam(String id) {
            this.id = id;
        }

        public static ClinicalAnalystParam of(ClinicalAnalyst clinicalAnalyst) {
            return new ClinicalAnalystParam(clinicalAnalyst.getId());
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ClinicalAnalystParam{");
            sb.append("id='").append(id).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public ClinicalAnalystParam setId(String id) {
            this.id = id;
            return this;
        }
    }
}
