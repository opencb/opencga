package org.opencb.opencga.core.models.file;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.LinkedList;
import java.util.List;

public class MissingSamples {

    @DataField(id = "existing", indexed = true,
            description = FieldConstants.MISSING_SAMPLE_EXISTING_DESCRIPTION)
    private List<String> existing;

    @DataField(id = "nonExisting", indexed = true,
            description = FieldConstants.MISSING_SAMPLE_NON_EXISTING_DESCRIPTION)
    private List<String> nonExisting;

    public MissingSamples() {
    }

    public MissingSamples(List<String> existing, List<String> nonExisting) {
        this.existing = existing;
        this.nonExisting = nonExisting;
    }

    public static MissingSamples initialize() {
        return new MissingSamples(new LinkedList<>(), new LinkedList<>());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MissingSamples{");
        sb.append("existing=").append(existing);
        sb.append(", nonExisting=").append(nonExisting);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getExisting() {
        return existing;
    }

    public MissingSamples setExisting(List<String> existing) {
        this.existing = existing;
        return this;
    }

    public List<String> getNonExisting() {
        return nonExisting;
    }

    public MissingSamples setNonExisting(List<String> nonExisting) {
        this.nonExisting = nonExisting;
        return this;
    }
}
