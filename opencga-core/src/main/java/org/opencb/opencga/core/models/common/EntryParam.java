package org.opencb.opencga.core.models.common;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class EntryParam {

    @DataField(description = ParamConstants.ENTRY_PARAM_ID_DESCRIPTION)
    private String id;

    public EntryParam() {
    }

    public EntryParam(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public EntryParam setId(String id) {
        this.id = id;
        return this;
    }
}
