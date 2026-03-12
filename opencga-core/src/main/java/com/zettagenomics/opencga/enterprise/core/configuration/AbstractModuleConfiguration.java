package com.zettagenomics.opencga.enterprise.core.configuration;

public abstract class AbstractModuleConfiguration {

    protected boolean active;

    public AbstractModuleConfiguration() {
    }

    public AbstractModuleConfiguration(boolean active) {
        this.active = active;
    }

    public boolean isActive() {
        return active;
    }

    public AbstractModuleConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }
}
