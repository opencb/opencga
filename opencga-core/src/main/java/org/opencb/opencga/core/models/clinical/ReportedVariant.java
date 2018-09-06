package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;

import java.util.List;
import java.util.Map;

public class ReportedVariant extends Variant {

    private double deNovoQualityScore;
    private List<ReportedEvent> reportedEvents;
    private List<Comment> comments;

    // TODO maybe this parameter should be in Variant
    private Map<String, Object> attributes;

    public ReportedVariant() {
    }

    public ReportedVariant(VariantAvro avro) {
        super(avro);
    }

    public ReportedVariant(VariantAvro avro, double deNovoQualityScore, List<ReportedEvent> reportedEvents, List<Comment> comments,
                           Map<String, Object> attributes) {
        super(avro);

        this.deNovoQualityScore = deNovoQualityScore;
        this.reportedEvents = reportedEvents;
        this.comments = comments;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ReportedVariant{");
        sb.append("deNovoQualityScore=").append(deNovoQualityScore);
        sb.append(", reportedEvents=").append(reportedEvents);
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public double getDeNovoQualityScore() {
        return deNovoQualityScore;
    }

    public ReportedVariant setDeNovoQualityScore(double deNovoQualityScore) {
        this.deNovoQualityScore = deNovoQualityScore;
        return this;
    }

    public List<ReportedEvent> getReportedEvents() {
        return reportedEvents;
    }

    public ReportedVariant setReportedEvents(List<ReportedEvent> reportedEvents) {
        this.reportedEvents = reportedEvents;
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
