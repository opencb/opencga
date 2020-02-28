package org.opencb.opencga.core.models.project;

import org.opencb.opencga.core.models.file.File;

import java.util.Objects;

public class DataStores {

    private DataStore variant;
//    private DataStore alignment;
//    private DataStore expression;

    public DataStore getVariant() {
        return variant;
    }

    public DataStores setVariant(DataStore variant) {
        this.variant = variant;
        return this;
    }

    public DataStore getDataStore(File.Bioformat bioformat) {
        switch (bioformat) {
            case VARIANT:
                return variant;
            default:
                return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataStores that = (DataStores) o;
        return Objects.equals(variant, that.variant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variant);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DataStores{");
        sb.append("variant=").append(variant);
        sb.append('}');
        return sb.toString();
    }
}
