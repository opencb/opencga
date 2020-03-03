package org.opencb.opencga.core.models.clinical;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.*;
import java.util.stream.Collectors;

public class ClinicalAnalysisCreateParams {

    private String id;
    private String description;
    private ClinicalAnalysis.Type type;

    private Disorder disorder;

    private Map<String, List<String>> files;

    private ProbandParam proband;
    private FamilyParam family;
    private Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband;
    private ClinicalAnalystParam analyst;
    private ClinicalAnalysisInternal internal;
    private List<InterpretationCreateParams> interpretations;

    private ClinicalConsent consent;

    private String dueDate;
    private List<Comment> comments;
    private List<Alert> alerts;
    private Enums.Priority priority;
    private List<String> flags;

    private Map<String, Object> attributes;

    public ClinicalAnalysisCreateParams() {
    }

    public ClinicalAnalysisCreateParams(String id, String description, ClinicalAnalysis.Type type, Disorder disorder,
                                        Map<String, List<String>> files, ProbandParam proband, FamilyParam family,
                                        Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband, ClinicalAnalystParam analyst,
                                        ClinicalAnalysisInternal internal, List<InterpretationCreateParams> interpretations,
                                        ClinicalConsent consent, String dueDate, List<Comment> comments, List<Alert> alerts,
                                        Enums.Priority priority, List<String> flags, Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.roleToProband = roleToProband;
        this.analyst = analyst;
        this.internal = internal;
        this.interpretations = interpretations;
        this.consent = consent;
        this.dueDate = dueDate;
        this.comments = comments;
        this.alerts = alerts;
        this.priority = priority;
        this.flags = flags;
        this.attributes = attributes;
    }

    public static ClinicalAnalysisCreateParams of(ClinicalAnalysis clinicalAnalysis) {
        Map<String, List<String>> files;
        if (clinicalAnalysis.getFiles() != null) {
            files = new HashMap<>();
            for (Map.Entry<String, List<File>> entry : clinicalAnalysis.getFiles().entrySet()) {
                List<String> tmpFiles = new ArrayList<>();
                if (entry.getValue() != null) {
                    for (File file : entry.getValue()) {
                        if (StringUtils.isNotEmpty(file.getPath())) {
                            tmpFiles.add(file.getPath());
                        } else {
                            tmpFiles.add(file.getUuid());
                        }
                    }
                }
                if (StringUtils.isNotEmpty(entry.getKey())) {
                    files.put(entry.getKey(), tmpFiles);
                }
            }
        } else {
            files = Collections.emptyMap();
        }
        return new ClinicalAnalysisCreateParams(clinicalAnalysis.getId(), clinicalAnalysis.getDescription(),
                clinicalAnalysis.getType(), clinicalAnalysis.getDisorder(), files,
                clinicalAnalysis.getProband() != null ? ProbandParam.of(clinicalAnalysis.getProband()) : null,
                clinicalAnalysis.getFamily() != null ? FamilyParam.of(clinicalAnalysis.getFamily()) : null,
                clinicalAnalysis.getRoleToProband(),
                clinicalAnalysis.getAnalyst() != null ? ClinicalAnalystParam.of(clinicalAnalysis.getAnalyst()) : null,
                clinicalAnalysis.getInternal(),
                clinicalAnalysis.getInterpretations() != null
                        ? clinicalAnalysis.getInterpretations().stream().map(InterpretationCreateParams::of).collect(Collectors.toList())
                        : Collections.emptyList(), clinicalAnalysis.getConsent(), clinicalAnalysis.getDueDate(),
                clinicalAnalysis.getComments(), clinicalAnalysis.getAlerts(), clinicalAnalysis.getPriority(), clinicalAnalysis.getFlags(),
                clinicalAnalysis.getAttributes());
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
        sb.append(", roleToProband=").append(roleToProband);
        sb.append(", analyst=").append(analyst);
        sb.append(", internal=").append(internal);
        sb.append(", interpretations=").append(interpretations);
        sb.append(", consent=").append(consent);
        sb.append(", dueDate='").append(dueDate).append('\'');
        sb.append(", comments=").append(comments);
        sb.append(", alerts=").append(alerts);
        sb.append(", priority=").append(priority);
        sb.append(", flags=").append(flags);
        sb.append(", attributes=").append(attributes);
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

        Map<String, List<File>> fileMap = new HashMap<>();
        if (files != null) {
            for (Map.Entry<String, List<String>> entry : files.entrySet()) {
                List<File> fileList = entry.getValue().stream().map(fileId -> new File().setId(fileId)).collect(Collectors.toList());
                fileMap.put(entry.getKey(), fileList);
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

        List<Interpretation> interpretationList =
                interpretations != null
                        ? interpretations.stream()
                        .map(InterpretationCreateParams::toClinicalInterpretation)
                        .collect(Collectors.toList())
                        : new ArrayList<>();
        String assignee = analyst != null ? analyst.assignee : "";
        return new ClinicalAnalysis(id, description, type, disorder, fileMap, individual, f, roleToProband, consent, interpretationList,
                priority, new ClinicalAnalysisAnalyst(assignee, ""), flags, null, dueDate, comments, alerts, internal, 1, attributes);
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

    public Disorder getDisorder() {
        return disorder;
    }

    public ClinicalAnalysisCreateParams setDisorder(Disorder disorder) {
        this.disorder = disorder;
        return this;
    }

    public Map<String, List<String>> getFiles() {
        return files;
    }

    public ClinicalAnalysisCreateParams setFiles(Map<String, List<String>> files) {
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

    public Map<String, ClinicalAnalysis.FamiliarRelationship> getRoleToProband() {
        return roleToProband;
    }

    public ClinicalAnalysisCreateParams setRoleToProband(Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband) {
        this.roleToProband = roleToProband;
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

    public List<InterpretationCreateParams> getInterpretations() {
        return interpretations;
    }

    public ClinicalAnalysisCreateParams setInterpretations(List<InterpretationCreateParams> interpretations) {
        this.interpretations = interpretations;
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

    public List<Comment> getComments() {
        return comments;
    }

    public ClinicalAnalysisCreateParams setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }

    public List<Alert> getAlerts() {
        return alerts;
    }

    public ClinicalAnalysisCreateParams setAlerts(List<Alert> alerts) {
        this.alerts = alerts;
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
        private String assignee;

        public ClinicalAnalystParam() {
        }

        public ClinicalAnalystParam(String assignee) {
            this.assignee = assignee;
        }

        public static ClinicalAnalystParam of(ClinicalAnalysisAnalyst clinicalAnalyst) {
            return new ClinicalAnalystParam(clinicalAnalyst.getAssignee());
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ClinicalAnalystParam{");
            sb.append("assignee='").append(assignee).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getAssignee() {
            return assignee;
        }

        public ClinicalAnalystParam setAssignee(String assignee) {
            this.assignee = assignee;
            return this;
        }
    }
}
