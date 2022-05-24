package org.opencb.opencga.test.config;

import java.util.List;

public class Variant {
    private String chromosome;
    private boolean skip;
    private String position;
    private String reference;
    private String alternate;
    private String quality;
    private String filter;
    private String info;
    private String format;
    private String id;
    private List<String> samples;

    public Variant() {
    }


    @Override
    public String toString() {
        return "Variant{" +
                "chromosome='" + chromosome + '\'' +
                ", skip=" + skip +
                ", position='" + position + '\'' +
                ", reference='" + reference + '\'' +
                ", id='" + id + '\'' +
                ", alternate='" + alternate + '\'' +
                ", quality='" + quality + '\'' +
                ", filter='" + filter + '\'' +
                ", info='" + info + '\'' +
                ", format='" + format + '\'' +
                ", samples=" + samples +
                '}';
    }


    public String getChromosome() {
        return chromosome;
    }

    public Variant setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public String getPosition() {
        return position;
    }

    public Variant setPosition(String position) {
        this.position = position;
        return this;
    }

    public String getReference() {
        return reference;
    }

    public Variant setReference(String reference) {
        this.reference = reference;
        return this;
    }

    public String getAlternate() {
        return alternate;
    }

    public Variant setAlternate(String alternate) {
        this.alternate = alternate;
        return this;
    }

    public String getQuality() {
        return quality;
    }

    public Variant setQuality(String quality) {
        this.quality = quality;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public Variant setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getInfo() {
        return info;
    }

    public Variant setInfo(String info) {
        this.info = info;
        return this;
    }

    public String getFormat() {
        return format;
    }

    public Variant setFormat(String format) {
        this.format = format;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public Variant setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public boolean isSkip() {
        return skip;
    }

    public Variant setSkip(boolean skip) {
        this.skip = skip;
        return this;
    }

    public String getId() {
        return id;
    }

    public Variant setId(String id) {
        this.id = id;
        return this;
    }
}
