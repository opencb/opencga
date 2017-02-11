package org.opencb.opencga.storage.core.search;

import org.apache.solr.client.solrj.beans.Field;

import java.util.*;

/**
 * Created by wasim on 09/11/16.
 */

/**
 *  I M P O R T A N T:
 *
 * In order to insert VariantSearch objects into your solr cores/collections you must
 * add the below fields in the the file schema.xml located in the core/collection folder.
 *
 <field name="dbSNP" type="string" indexed="true" stored="true" multiValued="false"/>
 <field name="chromosome" type="string" indexed="true" stored="true" multiValued="false"/>
 <field name="start" type="int" indexed="true" stored="true" multiValued="false"/>
 <field name="end" type="int" indexed="true" stored="true" multiValued="false"/>
 <field name="xrefs" type="string" indexed="true" stored="true" multiValued="true"/>
 <field name="type" type="string" indexed="true" stored="true" multiValued="false"/>
 <field name="phastCons" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="phylop" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="gerp" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="caddRaw" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="caddScaled" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="sift" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="polyphen" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="clinvar" type="text_en" indexed="true" stored="true" multiValued="true"/>
 <field name="genes" type="string" indexed="true" stored="true" multiValued="true"/>
 <field name="soAcc" type="int" indexed="true" stored="true" multiValued="true"/>
 <field name="geneToSoAcc" type="string" indexed="true" stored="true" multiValued="true"/>
 <dynamicField name="popFreq_*" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="studies" type="string" indexed="true" stored="true" multiValued="true"/>
 */

public class VariantSearch {

    @Field
    private String id;

    @Field("dbSNP")
    private String dbSNP;

    @Field("chromosome")
    private String chromosome;

    @Field("start")
    private int start;

    @Field("end")
    private int end;

    @Field("xrefs")
    private Set<String> xrefs;

    @Field("type")
    private String type;

    @Field("phastCons")
    private double phastCons;

    @Field("phylop")
    private double phylop;

    @Field("gerp")
    private double gerp;

    @Field("caddRaw")
    private double caddRaw;

    @Field("caddScaled")
    private double caddScaled;

    @Field("sift")
    private double sift;

    @Field("polyphen")
    private double polyphen;

    @Field("clinvar")
    private Set<String> clinvar;

    @Field("genes")
    private Set<String> genes;

    @Field("soAcc")
    private Set<Integer> soAcc;

    @Field("geneToSoAcc")
    private Set<String> geneToSoAcc;

    @Field("popFreq_*")
    private Map<String, Float> popFreq;

    @Field("studies")
    private List<String> studies;


    public VariantSearch() {
        this.genes = new HashSet<>();
        this.soAcc = new HashSet<>();
        this.geneToSoAcc = new HashSet<>();
        this.popFreq = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantSearch{");
        sb.append("id='").append(id).append('\'');
        sb.append(", dbSNP='").append(dbSNP).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", xrefs=").append(xrefs);
        sb.append(", type='").append(type).append('\'');
        sb.append(", phastCons=").append(phastCons);
        sb.append(", phylop=").append(phylop);
        sb.append(", gerp=").append(gerp);
        sb.append(", caddRaw=").append(caddRaw);
        sb.append(", caddScaled=").append(caddScaled);
        sb.append(", sift=").append(sift);
        sb.append(", polyphen=").append(polyphen);
        sb.append(", clinvar=").append(clinvar);
        sb.append(", genes=").append(genes);
        sb.append(", soAcc=").append(soAcc);
        sb.append(", geneToSoAcc=").append(geneToSoAcc);
        sb.append(", popFreq=").append(popFreq);
        sb.append(", studies=").append(studies);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public VariantSearch setId(String id) {
        this.id = id;
        return this;
    }

    public String getDbSNP() {
        return dbSNP;
    }

    public VariantSearch setDbSNP(String dbSNP) {
        this.dbSNP = dbSNP;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public VariantSearch setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public VariantSearch setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public VariantSearch setEnd(int end) {
        this.end = end;
        return this;
    }

    public Set<String> getXrefs() {
        return xrefs;
    }

    public VariantSearch setXrefs(Set<String> xrefs) {
        this.xrefs = xrefs;
        return this;
    }

    public String getType() {
        return type;
    }

    public VariantSearch setType(String type) {
        this.type = type;
        return this;
    }

    public double getPhastCons() {
        return phastCons;
    }

    public VariantSearch setPhastCons(double phastCons) {
        this.phastCons = phastCons;
        return this;
    }

    public double getPhylop() {
        return phylop;
    }

    public VariantSearch setPhylop(double phylop) {
        this.phylop = phylop;
        return this;
    }

    public double getGerp() {
        return gerp;
    }

    public VariantSearch setGerp(double gerp) {
        this.gerp = gerp;
        return this;
    }

    public double getCaddRaw() {
        return caddRaw;
    }

    public VariantSearch setCaddRaw(double caddRaw) {
        this.caddRaw = caddRaw;
        return this;
    }

    public double getCaddScaled() {
        return caddScaled;
    }

    public VariantSearch setCaddScaled(double caddScaled) {
        this.caddScaled = caddScaled;
        return this;
    }

    public double getSift() {
        return sift;
    }

    public VariantSearch setSift(double sift) {
        this.sift = sift;
        return this;
    }

    public double getPolyphen() {
        return polyphen;
    }

    public VariantSearch setPolyphen(double polyphen) {
        this.polyphen = polyphen;
        return this;
    }

    public Set<String> getClinvar() {
        return clinvar;
    }

    public VariantSearch setClinvar(Set<String> clinvar) {
        this.clinvar = clinvar;
        return this;
    }

    public Set<String> getGenes() {
        return genes;
    }

    public VariantSearch setGenes(Set<String> genes) {
        this.genes = genes;
        return this;
    }

    public Set<Integer> getSoAcc() {
        return soAcc;
    }

    public VariantSearch setSoAcc(Set<Integer> soAcc) {
        this.soAcc = soAcc;
        return this;
    }

    public Set<String> getGeneToSoAcc() {
        return geneToSoAcc;
    }

    public VariantSearch setGeneToSoAcc(Set<String> geneToSoAcc) {
        this.geneToSoAcc = geneToSoAcc;
        return this;
    }

    public Map<String, Float> getPopFreq() {
        return popFreq;
    }

    public VariantSearch setPopFreq(Map<String, Float> popFreq) {
        this.popFreq = popFreq;
        return this;
    }

    public List<String> getStudies() {
        return studies;
    }

    public VariantSearch setStudies(List<String> studies) {
        this.studies = studies;
        return this;
    }
}

