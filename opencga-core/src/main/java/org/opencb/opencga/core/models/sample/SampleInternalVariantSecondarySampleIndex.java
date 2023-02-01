package org.opencb.opencga.core.models.sample;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.Objects;

public class SampleInternalVariantSecondarySampleIndex {

    private IndexStatus status;
    private IndexStatus familyStatus;
    private Integer version;

    public SampleInternalVariantSecondarySampleIndex() {
    }

    @Deprecated
    public SampleInternalVariantSecondarySampleIndex(IndexStatus status) {
        this.status = status;
    }

    public SampleInternalVariantSecondarySampleIndex(IndexStatus status, IndexStatus familyStatus, Integer version) {
        this.status = status;
        this.familyStatus = familyStatus;
        this.version = version;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("status", status)
                .append("familyStatus", familyStatus)
                .append("version", version)
                .toString();
    }

    public IndexStatus getStatus() {
        return status;
    }

    public SampleInternalVariantSecondarySampleIndex setStatus(IndexStatus status) {
        this.status = status;
        return this;
    }

    public IndexStatus getFamilyStatus() {
        return familyStatus;
    }

    public SampleInternalVariantSecondarySampleIndex setFamilyStatus(IndexStatus familyStatus) {
        this.familyStatus = familyStatus;
        return this;
    }

    public Integer getVersion() {
        return version;
    }

    public SampleInternalVariantSecondarySampleIndex setVersion(Integer version) {
        this.version = version;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleInternalVariantSecondarySampleIndex that = (SampleInternalVariantSecondarySampleIndex) o;
        return Objects.equals(status, that.status) && Objects.equals(familyStatus, that.familyStatus) && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, familyStatus, version);
    }
}
