package org.opencb.opencga.core.models.study;

public class CatalogServiceConfiguration {

    private boolean active;

    public CatalogServiceConfiguration() {
    }

    public CatalogServiceConfiguration(boolean active) {
        this.active = active;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CatalogServiceConfiguration{");
        sb.append("active=").append(active);
        sb.append('}');
        return sb.toString();
    }

    public boolean isActive() {
        return active;
    }

    public CatalogServiceConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }
}
