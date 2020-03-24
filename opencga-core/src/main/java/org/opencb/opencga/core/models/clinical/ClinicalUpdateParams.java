package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class ClinicalUpdateParams {

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
    private List<InterpretationUpdateParams> interpretations;

    private ClinicalConsent consent;

    private String dueDate;
    private List<Comment> comments;
    private List<Alert> alerts;
    private Enums.Priority priority;
    private List<String> flags;

    private Map<String, Object> attributes;
    private CustomStatusParams status;

    public ClinicalUpdateParams() {
    }

    public ClinicalUpdateParams(String id, String description, ClinicalAnalysis.Type type, Disorder disorder,
                                Map<String, List<String>> files, ProbandParam proband, FamilyParam family,
                                Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband, ClinicalAnalystParam analyst,
                                ClinicalAnalysisInternal internal, List<InterpretationUpdateParams> interpretations,
                                ClinicalConsent consent, String dueDate, List<Comment> comments, List<Alert> alerts,
                                Enums.Priority priority, List<String> flags, Map<String, Object> attributes, CustomStatusParams status) {
        this.id = id;
        this.description = description;
        this.type = type;
        this.disorder = disorder;
        this.files = files;
        this.proband = proband;
        this.family = family;
        this.roleToProband = roleToProband;
        this.analyst = analyst;
        this.interpretations = interpretations;
        this.consent = consent;
        this.dueDate = dueDate;
        this.comments = comments;
        this.alerts = alerts;
        this.priority = priority;
        this.flags = flags;
        this.internal = internal;
        this.attributes = attributes;
        this.status = status;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalUpdateParams{");
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
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public static class SampleParams {
        private String id;

        public SampleParams() {
        }

        public String getId() {
            return id;
        }

        public SampleParams setId(String id) {
            this.id = id;
            return this;
        }
    }

    public static class ProbandParam {
        private String id;
        private List<SampleParams> samples;

        public ProbandParam() {
        }

        public Individual toUncheckedIndividual() {
            Individual individual = new Individual().setId(id);
            if (ListUtils.isNotEmpty(samples)) {
                List<Sample> sampleList = new ArrayList<>(samples.size());
                for (SampleParams sample : samples) {
                    sampleList.add(new Sample().setId(sample.getId()));
                }
                individual.setSamples(sampleList);
            }
            return  individual;
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

    public static class FamilyParam {
        private String id;
        private List<ProbandParam> members;

        public FamilyParam() {
        }

        public Family toUncheckedFamily() {
            List<Individual> memberList = null;
            if (ListUtils.isNotEmpty(members)) {
                memberList = members.stream().map(ProbandParam::toUncheckedIndividual).collect(Collectors.toList());
            }
            return new Family()
                    .setId(id)
                    .setMembers(memberList);
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

    public static class ClinicalAnalystParam {
        private String assignee;

        public ClinicalAnalystParam(String assignee) {
            this.assignee = assignee;
        }

        public String getAssignee() {
            return assignee;
        }

        public ClinicalAnalystParam setAssignee(String assignee) {
            this.assignee = assignee;
            return this;
        }
    }

    public String getId() {
        return id;
    }

    public ClinicalUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalAnalysis.Type getType() {
        return type;
    }

    public ClinicalUpdateParams setType(ClinicalAnalysis.Type type) {
        this.type = type;
        return this;
    }

    public Disorder getDisorder() {
        return disorder;
    }

    public ClinicalUpdateParams setDisorder(Disorder disorder) {
        this.disorder = disorder;
        return this;
    }

    public Map<String, List<String>> getFiles() {
        return files;
    }

    public ClinicalUpdateParams setFiles(Map<String, List<String>> files) {
        this.files = files;
        return this;
    }

    public ProbandParam getProband() {
        return proband;
    }

    public ClinicalUpdateParams setProband(ProbandParam proband) {
        this.proband = proband;
        return this;
    }

    public FamilyParam getFamily() {
        return family;
    }

    public ClinicalUpdateParams setFamily(FamilyParam family) {
        this.family = family;
        return this;
    }

    public Map<String, ClinicalAnalysis.FamiliarRelationship> getRoleToProband() {
        return roleToProband;
    }

    public ClinicalUpdateParams setRoleToProband(Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband) {
        this.roleToProband = roleToProband;
        return this;
    }

    public ClinicalAnalystParam getAnalyst() {
        return analyst;
    }

    public ClinicalUpdateParams setAnalyst(ClinicalAnalystParam analyst) {
        this.analyst = analyst;
        return this;
    }

    public List<InterpretationUpdateParams> getInterpretations() {
        return interpretations;
    }

    public ClinicalUpdateParams setInterpretations(List<InterpretationUpdateParams> interpretations) {
        this.interpretations = interpretations;
        return this;
    }

    public ClinicalConsent getConsent() {
        return consent;
    }

    public ClinicalUpdateParams setConsent(ClinicalConsent consent) {
        this.consent = consent;
        return this;
    }

    public String getDueDate() {
        return dueDate;
    }

    public ClinicalUpdateParams setDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public ClinicalUpdateParams setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }

    public List<Alert> getAlerts() {
        return alerts;
    }

    public ClinicalUpdateParams setAlerts(List<Alert> alerts) {
        this.alerts = alerts;
        return this;
    }

    public Enums.Priority getPriority() {
        return priority;
    }

    public ClinicalUpdateParams setPriority(Enums.Priority priority) {
        this.priority = priority;
        return this;
    }

    public List<String> getFlags() {
        return flags;
    }

    public ClinicalUpdateParams setFlags(List<String> flags) {
        this.flags = flags;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClinicalUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public ClinicalAnalysisInternal getInternal() {
        return internal;
    }

    public ClinicalUpdateParams setInternal(ClinicalAnalysisInternal internal) {
        this.internal = internal;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public ClinicalUpdateParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }
}
