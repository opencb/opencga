package org.opencb.opencga.storage.core.search;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 09/11/16.
 */

public class VariantSearch {

    @Field
    private String id;
    @Field
    private String dbSNP;
    @Field
    private String type;
    @Field
    private String chromosome;
    @Field
    private int start;
    @Field
    private int end;
    @Field
    private double gerp;
    @Field
    private double caddRaw;
    @Field
    private double caddScaled;
    @Field
    private double phastCons;
    @Field
    private double phylop;
    @Field
    private double sift;
    @Field
    private double polyphen;
    @Field
    private String[] studies;
    @Field
    private List<String> genes;
    @Field
    private List<String> accessions;
    @Field("study_*")
    private Map<String, Float> populations;


    public VariantSearch() {
        this.accessions = new ArrayList<>();
        this.genes = new ArrayList<>();
        this.populations = new HashMap<String, Float>();
    }

    public String getDbSNP() {
        return dbSNP;
    }

    @Field
    public VariantSearch setDbSNP(String dbSNP) {
        this.dbSNP = dbSNP;
        return this;
    }

    public double getCaddRaw() {
        return caddRaw;
    }

    public void setCaddRaw(double caddRaw) {
        this.caddRaw = caddRaw;
    }

    public double getCaddScaled() {
        return caddScaled;
    }

    public void setCaddScaled(double caddScaled) {
        this.caddScaled = caddScaled;
    }

    public double getGerp() {
        return gerp;
    }

    public void setGerp(double gerp) {
        this.gerp = gerp;
    }

    public double getPhastCons() {
        return phastCons;
    }

    public void setPhastCons(double phastCons) {
        this.phastCons = phastCons;
    }

    public double getPhylop() {
        return phylop;
    }

    public void setPhylop(double phylop) {
        this.phylop = phylop;
    }

    public double getSift() {
        return sift;
    }

    public void setSift(double sift) {
        this.sift = sift;
    }

    public double getPolyphen() {
        return polyphen;
    }

    public void setPolyphen(double polyphen) {
        this.polyphen = polyphen;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public List<String> getGenes() {
        return genes;
    }

    public void setGenes(String geneName) {
        this.genes.add(geneName);
    }

    public void setGenes(List<String> geneNames) {
        this.genes.addAll(geneNames);
    }

    public List<String> getAccessions() {
        return accessions;
    }

    public void setAccessions(String accession) {
        this.accessions.add(accession);
    }

    public void setAccessions(List<String> accessions) {
        this.accessions.addAll(accessions);
    }

    public Map<String, Float> getPopulations() {
        return populations;
    }

    public void setPopulations(Map<String, Float> populations) {
        this.populations.putAll(populations);
    }

    public String[] getStudies() {
        return studies;
    }

    public void setStudies(String[] studies) {
        this.studies = studies;
    }

    @Override
    public String toString() {
        return "VariantSearch{"
                + "id='" + id + '\''
                + ", dbSNP='" + dbSNP + '\''
                + ", type='" + type + '\''
                + ", chromosome='" + chromosome + '\''
                + ", start=" + start
                + ", end=" + end
                + ", gerp=" + gerp
                + ", caddRaw=" + caddRaw
                + ", caddScaled=" + caddScaled
                + ", phastCons=" + phastCons
                + ", phylop=" + phylop
                + ", sift=" + sift
                + ", polyphen=" + polyphen
                + ", studies=" + studies
                + ", genes=" + genes
                + ", accessions=" + accessions
                + ", populations=" + populations
                + '}';
    }
}

