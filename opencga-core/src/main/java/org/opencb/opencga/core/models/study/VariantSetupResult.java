package org.opencb.opencga.core.models.study;

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;

public class VariantSetupResult {

    @DataField(description = "User ID that started the setup run")
    private String userId;
    @DataField(description = "Date when the variant setup was executed")
    private String date;
    @DataField(description = "Variant setup status")
    private Status status;
    @DataField(description = "Input params for the variant setup")
    private ObjectMap params;

    @DataField(description = "Generated variant storage configuration options given the input params.")
    private ObjectMap options;

    public enum Status {
        READY,
        NOT_READY
    }

    public VariantSetupResult() {
    }

    public String getUserId() {
        return userId;
    }

    public VariantSetupResult setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getDate() {
        return date;
    }

    public VariantSetupResult setDate(String date) {
        this.date = date;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public VariantSetupResult setStatus(Status status) {
        this.status = status;
        return this;
    }

    public ObjectMap getParams() {
        return params;
    }

    public VariantSetupResult setParams(ObjectMap params) {
        this.params = params;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public VariantSetupResult setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }


}
