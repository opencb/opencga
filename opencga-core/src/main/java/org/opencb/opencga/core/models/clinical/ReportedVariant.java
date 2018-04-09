package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;

import java.util.List;
import java.util.Map;

public class ReportedVariant extends Variant {

    private List<ReportedEvent> reportedEvents;
    @Deprecated
    private List<CalledGenotype> calledGenotypes;
    private List<Comment> comments;
    private Map<String, Object> attributes;

    public ReportedVariant() {
    }

    public ReportedVariant(VariantAvro avro) {
        super(avro);
    }

    public ReportedVariant(VariantAvro avro, List<ReportedEvent> reportedEvents, List<CalledGenotype> calledGenotypes,
                           List<Comment> comments, Map<String, Object> attributes) {
        super(avro);

        this.reportedEvents = reportedEvents;
        this.calledGenotypes = calledGenotypes;
        this.comments = comments;
        this.attributes = attributes;
    }

//    public ReportedVariant(Variant variant) {
//    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ReportedVariant{");
        sb.append("reportedEvents=").append(reportedEvents);
        sb.append(", calledGenotypes=").append(calledGenotypes);
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public List<ReportedEvent> getReportedEvents() {
        return reportedEvents;
    }

    public ReportedVariant setReportedEvents(List<ReportedEvent> reportedEvents) {
        this.reportedEvents = reportedEvents;
        return this;
    }

    public List<CalledGenotype> getCalledGenotypes() {
        return calledGenotypes;
    }

    public ReportedVariant setCalledGenotypes(List<CalledGenotype> calledGenotypes) {
        this.calledGenotypes = calledGenotypes;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public ReportedVariant setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ReportedVariant setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
