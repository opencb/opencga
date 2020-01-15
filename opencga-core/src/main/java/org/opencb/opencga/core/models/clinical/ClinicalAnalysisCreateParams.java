package org.opencb.opencga.core.models.clinical;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClinicalAnalysisCreateParams {

    private String id;
    @Deprecated
    private String name;
    private String description;
    private ClinicalAnalysis.Type type;

    private Disorder disorder;

    private Map<String, List<String>> files;

    private ProbandParam proband;
    private FamilyParam family;
    private Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband;
    private ClinicalAnalystParam analyst;
    private ClinicalAnalysis.ClinicalStatus status;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", disorder=").append(disorder);
        sb.append(", files=").append(files);
        sb.append(", proband=").append(proband);
        sb.append(", family=").append(family);
        sb.append(", roleToProband=").append(roleToProband);
        sb.append(", analyst=").append(analyst);
        sb.append(", status=").append(status);
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
        String clinicalId = StringUtils.isEmpty(id) ? name : id;
        String assignee = analyst != null ? analyst.assignee : "";
        return new ClinicalAnalysis(clinicalId, description, type, disorder, fileMap, individual, f, roleToProband, consent,
                interpretationList, priority, new ClinicalAnalysis.ClinicalAnalyst(assignee, ""), flags, null,
                dueDate, comments, alerts, status, 1, attributes).setName(name);
    }

    public String getId() {
        return id;
    }

    public ClinicalAnalysisCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalAnalysisCreateParams setName(String name) {
        this.name = name;
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

    public ClinicalAnalysis.ClinicalStatus getStatus() {
        return status;
    }

    public ClinicalAnalysisCreateParams setStatus(ClinicalAnalysis.ClinicalStatus status) {
        this.status = status;
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
        public String id;
    }

    private static class ProbandParam {
        public String id;
        public List<SampleParams> samples;
    }

    private static class FamilyParam {
        public String id;
        public List<ProbandParam> members;
    }

    private static class ClinicalAnalystParam {
        public String assignee;
    }
}
