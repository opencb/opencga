package org.opencb.opencga.app.cli.config;

import java.util.Arrays;

public class CliUsage {

    CliCategory[] categories;

    public CliUsage() {
    }

    public CliUsage(CliCategory[] categories) {
        this.categories = categories;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Usage{");
        sb.append("categories=").append(Arrays.toString(categories));
        sb.append('}');
        return sb.toString();
    }

    public CliCategory[] getCategories() {
        return categories;
    }

    public CliUsage setCategories(CliCategory[] categories) {
        this.categories = categories;
        return this;
    }
}
