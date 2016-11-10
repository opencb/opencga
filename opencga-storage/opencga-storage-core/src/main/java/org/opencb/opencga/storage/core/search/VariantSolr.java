package org.opencb.opencga.storage.core.search;

import org.apache.solr.client.solrj.beans.Field;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by wasim on 09/11/16.
 */

public class VariantSolr {


    private String id;
    private String type;
    private String chromosome;
    private int Start;
    private int End;
    private double gerp;
    private double caddRaw;
    private double caddScaled;
    private double phastCons;
    private double phylop;
    private double sift;
    private double polyphen;
    private Set<String> geneNames;
    private Set<String> accessions;
    private Map<String, Float> populations;

    public VariantSolr() {
        this.accessions = new HashSet<String>();
        this.geneNames = new HashSet<String>();
        this.populations = new HashMap<String, Float>();
    }

    public double getCaddRaw() {
        return caddRaw;
    }

    @Field
    public void setCaddRaw(double caddRaw) {
        this.caddRaw = caddRaw;
    }

    public double getCaddScaled() {
        return caddScaled;
    }

    @Field
    public void setCaddScaled(double caddScaled) {
        this.caddScaled = caddScaled;
    }

    public double getGerp() {
        return gerp;
    }

    @Field
    public void setGerp(Double gerp) {
        this.gerp = gerp;
    }

    public double getPhastCons() {
        return phastCons;
    }

    @Field
    public void setPhastCons(Double phastCons) {
        this.phastCons = phastCons;
    }

    public double getPhylop() {
        return phylop;
    }

    @Field
    public void setPhylop(double phylop) {
        this.phylop = phylop;
    }


    public double getSift() {
        return sift;
    }

    @Field
    public void setSift(double sift) {
        this.sift = sift;
    }

    public double getPolyphen() {
        return polyphen;
    }

    @Field
    public void setPolyphen(double polyphen) {
        this.polyphen = polyphen;
    }

    public String getId() {
        return id;
    }

    @Field
    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    @Field
    public void setType(String type) {
        this.type = type;
    }

    public String getChromosome() {
        return chromosome;
    }

    @Field
    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getStart() {
        return Start;
    }

    @Field
    public void setStart(int start) {
        Start = start;
    }

    public int getEnd() {
        return End;
    }

    @Field
    public void setEnd(int end) {
        End = end;
    }

    public Set<String> getGeneNames() {
        return geneNames;
    }

    @Field
    public void setGeneNames(String geneName) {
        this.geneNames.add(geneName);
    }

    @Field
    public void setGeneNames(Set<String> geneNames) {
        this.geneNames.addAll(geneNames);
    }

    public Set<String> getAccessions() {
        return accessions;
    }

    @Field
    public void setAccessions(String accession) {
        this.accessions.add(accession);
    }

    @Field
    public void setAccessions(Set<String> accessions) {
        this.accessions.addAll(accessions);
    }

    public Map<String, Float> getPopulations() {
        return populations;
    }

    @Field("*")
    public void setPopulations(Map<String, Float> populations) {
        this.populations.putAll(populations);
    }
}

