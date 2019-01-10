package org.opencb.opencga.storage.core.metadata.models;

import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyMetadata {

    private int id;
    private String name;
    private Aggregation aggregation;
    private Long timeStamp;
    private VariantFileHeader variantHeader;

    private ObjectMap attributes;

    public StudyMetadata() {

    }

    public StudyMetadata(StudyConfiguration sc) {
        id = sc.getStudyId();
        name = sc.getStudyName();
        aggregation = sc.getAggregation();
        variantHeader = sc.getVariantHeader();
        timeStamp = sc.getTimeStamp();
        attributes = sc.getAttributes();
    }

    public int getId() {
        return id;
    }

    public StudyMetadata setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public StudyMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public Aggregation getAggregation() {
        return aggregation;
    }

    public void setAggregation(Aggregation aggregation) {
        this.aggregation = aggregation;
    }

    public void setAggregationStr(String aggregation) {
        this.aggregation = AggregationUtils.valueOf(aggregation);
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public StudyMetadata setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
        return this;
    }

    public VariantFileHeader getVariantHeader() {
        return variantHeader;
    }

    public StudyMetadata setVariantHeader(VariantFileHeader variantHeader) {
        this.variantHeader = variantHeader;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public StudyMetadata setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
