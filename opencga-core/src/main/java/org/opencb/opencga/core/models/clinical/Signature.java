package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;

public class Signature {

    @DataField(id = "signature", description = "Signature")
    private String signature;

    @DataField(id = "signedBy", description = "Name of the person who signed the report")
    private String signedBy;

    @DataField(id = "date", description = "Date when the report was signed")
    private String date;

    @DataField(id = "role", description = "Role of the person who signed the report")
    private String role;

    public Signature() {
    }

    public Signature(String signature, String signedBy, String date, String role) {
        this.signature = signature;
        this.signedBy = signedBy;
        this.date = date;
        this.role = role;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Signature{");
        sb.append("signature='").append(signature).append('\'');
        sb.append(", signedBy='").append(signedBy).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", role='").append(role).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSignature() {
        return signature;
    }

    public Signature setSignature(String signature) {
        this.signature = signature;
        return this;
    }

    public String getSignedBy() {
        return signedBy;
    }

    public Signature setSignedBy(String signedBy) {
        this.signedBy = signedBy;
        return this;
    }

    public String getDate() {
        return date;
    }

    public Signature setDate(String date) {
        this.date = date;
        return this;
    }

    public String getRole() {
        return role;
    }

    public Signature setRole(String role) {
        this.role = role;
        return this;
    }
}
