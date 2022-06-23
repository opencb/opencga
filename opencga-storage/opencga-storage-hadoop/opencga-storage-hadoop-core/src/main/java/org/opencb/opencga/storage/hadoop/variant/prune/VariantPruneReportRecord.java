package org.opencb.opencga.storage.hadoop.variant.prune;

import org.opencb.biodata.models.variant.Variant;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class VariantPruneReportRecord {
    private final Variant variant;
    private final Type type;
    private final List<Integer> studies;

    enum Type {
        FULL, PARTIAL
    }

    public VariantPruneReportRecord(Variant variant, Type type, List<Integer> studies) {
        this.variant = variant;
        this.type = type;
        this.studies = studies;
    }

    public VariantPruneReportRecord(String line) {
        String[] split = line.split("\t");
        variant = new Variant(split[0]);
        type = Type.valueOf(split[1]);
        studies = Arrays.stream(split[2].split(",")).map(Integer::valueOf).collect(Collectors.toList());
    }

    public Variant getVariant() {
        return variant;
    }

    public Type getType() {
        return type;
    }

    public List<Integer> getStudies() {
        return studies;
    }
}
