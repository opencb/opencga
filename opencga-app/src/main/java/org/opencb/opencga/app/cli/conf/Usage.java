package org.opencb.opencga.app.cli.conf;

import java.util.Arrays;

public class Usage {

    Category[] categories;

    public Usage() {
    }

    public Usage(Category[] categories) {
        this.categories = categories;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Usage{");
        sb.append("categories=").append(Arrays.toString(categories));
        sb.append('}');
        return sb.toString();
    }

    public Category[] getCategories() {
        return categories;
    }

    public Usage setCategories(Category[] categories) {
        this.categories = categories;
        return this;
    }
}
