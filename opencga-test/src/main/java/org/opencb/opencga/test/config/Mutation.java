package org.opencb.opencga.test.config;

import java.util.List;

public class Mutation {

    List<Variant> variants;
    private String file;
    private boolean skip;

    public Mutation() {
    }

    @Override
    public String toString() {
        return "Mutations{" +
                "variants=" + variants +
                ", file='" + file + '\'' +
                ", skip=" + skip +
                '}';
    }

    public List<Variant> getVariants() {
        return variants;
    }

    public Mutation setVariants(List<Variant> variants) {
        this.variants = variants;
        return this;
    }

    public String getFile() {
        return file;
    }

    public Mutation setFile(String file) {
        this.file = file;
        return this;
    }


    public boolean isSkip() {
        return skip;
    }

    public Mutation setSkip(boolean skip) {
        this.skip = skip;
        return this;
    }
}


