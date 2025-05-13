/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.search;

import org.apache.solr.client.solrj.beans.Field;
import org.opencb.biodata.models.variant.Variant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter.HASH_PREFIX;

/**
 * Created by wasim on 09/11/16.
 */

/**
 * VariantSearchModel must match the managed-schema file at opencga-storage/opencga-storage-core/src/main/resources.
 */
public class VariantSearchModel {

    @Field("id")
    private String id;

    @Field("variantId")
    private String variantId;

    @Field("chromosome")
    private String chromosome;

    @Field("start")
    private int start;

    @Field("end")
    private int end;

    @Field("xrefs")
    private List<String> xrefs;

    @Field("type")
    private String type;

    @Field("release")
    private int release;

    @Field("studies")
    private List<String> studies;

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

    @Field("siftDesc")
    private String siftDesc;

    @Field("polyphen")
    private double polyphen;

    @Field("polyphenDesc")
    private String polyphenDesc;

    @Field("genes")
    private List<String> genes;

    @Field("biotypes")
    private List<String> biotypes;

    @Field("soAcc")
    private List<Integer> soAcc;

    @Field("geneToSoAcc")
    private List<String> geneToSoAcc;

    @Field("clinicalSig")
    private List<String> clinicalSig;

    @Field("traits")
    private List<String> traits;

    @Field("other")
    private List<String> other;

    @Field("passStats_*")
    private Map<String, Float> passStats;

    @Field("altStats_*")
    private Map<String, Float> altStats;

    @Field("score_*")
    private Map<String, Float> score;

    @Field("scorePValue_*")
    private Map<String, Float> scorePValue;

    @Field("popFreq_*")
    private Map<String, Float> popFreq;

    @Field("attr_*")
    private Map<String, Object> attr;


    public static final double MISSING_VALUE = -100.0;

    public VariantSearchModel() {
        phastCons = MISSING_VALUE;
        phylop = MISSING_VALUE;
        gerp = MISSING_VALUE;
        caddRaw = MISSING_VALUE;
        caddScaled = MISSING_VALUE;
        sift = MISSING_VALUE;
        polyphen = MISSING_VALUE;

        this.xrefs = new ArrayList<>();
        this.studies = new ArrayList<>();
        this.genes = new ArrayList<>();
        this.biotypes = new ArrayList<>();
        this.soAcc = new ArrayList<>();
        this.geneToSoAcc = new ArrayList<>();
        this.traits = new ArrayList<>();
        this.other = new ArrayList<>();
        this.passStats = new HashMap<>();
        this.altStats = new HashMap<>();
        this.score = new HashMap<>();
        this.scorePValue = new HashMap<>();
        this.popFreq = new HashMap<>();
        this.attr = new HashMap<>();
    }

    public VariantSearchModel(VariantSearchModel init) {
        this.id = init.getId();
        this.variantId = init.getVariantId();
        this.chromosome = init.getChromosome();
        this.start = init.getStart();
        this.end = init.getEnd();
        this.xrefs = init.getXrefs();
        this.type = init.getType();
        this.release = init.getRelease();
        this.studies = init.getStudies();
        this.phastCons = init.getPhastCons();
        this.phylop = init.getPhylop();
        this.gerp = init.getGerp();
        this.caddRaw = init.getCaddRaw();
        this.caddScaled = init.getCaddScaled();
        this.sift = init.getSift();
        this.siftDesc = init.getSiftDesc();
        this.polyphen = init.getPolyphen();
        this.polyphenDesc = init.getPolyphenDesc();
        this.genes = init.getGenes();
        this.biotypes = init.getBiotypes();
        this.soAcc = init.getSoAcc();
        this.geneToSoAcc = init.geneToSoAcc;
        this.clinicalSig = init.getClinicalSig();
        this.traits = init.getTraits();
        this.other = init.getOther();
        this.passStats = init.getPassStats();
        this.altStats = init.getAltStats();
        this.score = init.getScore();
        this.scorePValue = init.getScorePValue();
        this.popFreq = init.getPopFreq();
        this.attr = init.getAttr();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantSearchModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", variantId='").append(variantId).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", xrefs=").append(xrefs);
        sb.append(", type='").append(type).append('\'');
        sb.append(", release=").append(release);
        sb.append(", studies=").append(studies);
        sb.append(", phastCons=").append(phastCons);
        sb.append(", phylop=").append(phylop);
        sb.append(", gerp=").append(gerp);
        sb.append(", caddRaw=").append(caddRaw);
        sb.append(", caddScaled=").append(caddScaled);
        sb.append(", sift=").append(sift);
        sb.append(", siftDesc='").append(siftDesc).append('\'');
        sb.append(", polyphen=").append(polyphen);
        sb.append(", polyphenDesc='").append(polyphenDesc).append('\'');
        sb.append(", genes=").append(genes);
        sb.append(", biotypes=").append(biotypes);
        sb.append(", soAcc=").append(soAcc);
        sb.append(", geneToSoAcc=").append(geneToSoAcc);
        sb.append(", clinicalSig=").append(clinicalSig);
        sb.append(", traits=").append(traits);
        sb.append(", other=").append(other);
        sb.append(", passStats=").append(passStats);
        sb.append(", altStats=").append(altStats);
        sb.append(", score=").append(score);
        sb.append(", scorePValue=").append(scorePValue);
        sb.append(", popFreq=").append(popFreq);
        sb.append(", attr=").append(attr);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Variant toVariantSimple() {
        String variantId = getId();
        if (variantId.startsWith(HASH_PREFIX)) {
            Object o = getAttr().get("attr_id");
            variantId = o instanceof String ? (String) o : ((List<String>) o).get(0);
        }
        Variant variant = new Variant(variantId);
        variant.setId(variantId);
        return variant;
    }

    public VariantSearchModel setId(String id) {
        this.id = id;
        return this;
    }

    public String getVariantId() {
        return variantId;
    }

    public VariantSearchModel setVariantId(String variantId) {
        this.variantId = variantId;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public VariantSearchModel setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public VariantSearchModel setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public VariantSearchModel setEnd(int end) {
        this.end = end;
        return this;
    }

    public List<String> getXrefs() {
        return xrefs;
    }

    public VariantSearchModel setXrefs(List<String> xrefs) {
        this.xrefs = xrefs;
        return this;
    }

    public String getType() {
        return type;
    }

    public VariantSearchModel setType(String type) {
        this.type = type;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public VariantSearchModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public List<String> getStudies() {
        return studies;
    }

    public VariantSearchModel setStudies(List<String> studies) {
        this.studies = studies;
        return this;
    }

    public double getPhastCons() {
        return phastCons;
    }

    public VariantSearchModel setPhastCons(double phastCons) {
        this.phastCons = phastCons;
        return this;
    }

    public double getPhylop() {
        return phylop;
    }

    public VariantSearchModel setPhylop(double phylop) {
        this.phylop = phylop;
        return this;
    }

    public double getGerp() {
        return gerp;
    }

    public VariantSearchModel setGerp(double gerp) {
        this.gerp = gerp;
        return this;
    }

    public double getCaddRaw() {
        return caddRaw;
    }

    public VariantSearchModel setCaddRaw(double caddRaw) {
        this.caddRaw = caddRaw;
        return this;
    }

    public double getCaddScaled() {
        return caddScaled;
    }

    public VariantSearchModel setCaddScaled(double caddScaled) {
        this.caddScaled = caddScaled;
        return this;
    }

    public double getSift() {
        return sift;
    }

    public VariantSearchModel setSift(double sift) {
        this.sift = sift;
        return this;
    }

    public String getSiftDesc() {
        return siftDesc;
    }

    public VariantSearchModel setSiftDesc(String siftDesc) {
        this.siftDesc = siftDesc;
        return this;
    }

    public double getPolyphen() {
        return polyphen;
    }

    public VariantSearchModel setPolyphen(double polyphen) {
        this.polyphen = polyphen;
        return this;
    }

    public String getPolyphenDesc() {
        return polyphenDesc;
    }

    public VariantSearchModel setPolyphenDesc(String polyphenDesc) {
        this.polyphenDesc = polyphenDesc;
        return this;
    }

    public List<String> getGenes() {
        return genes;
    }

    public VariantSearchModel setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public List<String> getBiotypes() {
        return biotypes;
    }

    public VariantSearchModel setBiotypes(List<String> biotypes) {
        this.biotypes = biotypes;
        return this;
    }

    public List<Integer> getSoAcc() {
        return soAcc;
    }

    public VariantSearchModel setSoAcc(List<Integer> soAcc) {
        this.soAcc = soAcc;
        return this;
    }

    public List<String> getGeneToSoAcc() {
        return geneToSoAcc;
    }

    public VariantSearchModel setGeneToSoAcc(List<String> geneToSoAcc) {
        this.geneToSoAcc = geneToSoAcc;
        return this;
    }

    public List<String> getClinicalSig() {
        return clinicalSig;
    }

    public VariantSearchModel setClinicalSig(List<String> clinicalSig) {
        this.clinicalSig = clinicalSig;
        return this;
    }

    public List<String> getTraits() {
        return traits;
    }

    public VariantSearchModel setTraits(List<String> traits) {
        this.traits = traits;
        return this;
    }

    public List<String> getOther() {
        return other;
    }

    public VariantSearchModel setOther(List<String> other) {
        this.other = other;
        return this;
    }

    public Map<String, Float> getPassStats() {
        return passStats;
    }

    public VariantSearchModel setPassStats(Map<String, Float> passStats) {
        this.passStats = passStats;
        return this;
    }

    public Map<String, Float> getAltStats() {
        return altStats;
    }

    public VariantSearchModel setAltStats(Map<String, Float> altStats) {
        this.altStats = altStats;
        return this;
    }

    public Map<String, Float> getScore() {
        return score;
    }

    public VariantSearchModel setScore(Map<String, Float> score) {
        this.score = score;
        return this;
    }

    public Map<String, Float> getScorePValue() {
        return scorePValue;
    }

    public VariantSearchModel setScorePValue(Map<String, Float> scorePValue) {
        this.scorePValue = scorePValue;
        return this;
    }

    public Map<String, Float> getPopFreq() {
        return popFreq;
    }

    public VariantSearchModel setPopFreq(Map<String, Float> popFreq) {
        this.popFreq = popFreq;
        return this;
    }

    public Map<String, Object> getAttr() {
        return attr;
    }

    public VariantSearchModel setAttr(Map<String, Object> attr) {
        this.attr = attr;
        return this;
    }
}
