package org.opencb.opencga.test.config;

import java.util.List;

public class Mutator {

    private List<Mutation> mutations;
    private boolean skip;

    public Mutator() {
    }

    @Override
    public String toString() {
        return "Mutator{" +
                "mutations=" + mutations +
                ", skip=" + skip +
                '}';
    }

    public boolean isSkip() {
        return skip;
    }

    public Mutator setSkip(boolean skip) {
        this.skip = skip;
        return this;
    }

    public List<Mutation> getMutations() {
        return mutations;
    }

    public Mutator setMutations(List<Mutation> mutations) {
        this.mutations = mutations;
        return this;
    }
}
