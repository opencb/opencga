package org.opencb.opencga.storage.benchmark.variant.queries;

/**
 * Created by wasim on 01/11/18.
 */
public class Chromosome {
    private String chromosome;
    private int start;
    private int end;


    public String getChromosome() {
        return chromosome;
    }

    public Chromosome setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public Chromosome setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public Chromosome setEnd(int end) {
        this.end = end;
        return this;
    }
}
