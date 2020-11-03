package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.FlagAnnotation;

public class FlagValueParam {

    private String id;

    public FlagValueParam() {
    }

    public FlagValueParam(String id) {
        this.id = id;
    }

    public static FlagValueParam of(FlagAnnotation flagAnnotation) {
        return flagAnnotation != null ? new FlagValueParam(flagAnnotation.getId()) : null;
    }

    public FlagAnnotation toFlagAnnotation() {
        return new FlagAnnotation(id, "", TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FlagValueParams{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FlagValueParam setId(String id) {
        this.id = id;
        return this;
    }
}
