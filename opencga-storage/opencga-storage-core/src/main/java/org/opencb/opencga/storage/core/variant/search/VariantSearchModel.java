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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 09/11/16.
 */

/**
 *  I M P O R T A N T:
 *
 * In order to insert VariantSearchModel objects into your solr cores you must
 * add the below fields in the the file schema.xml located in the core/collection folder. Or you
 * can use the solr_schema.xml file from opencga/opencga-storage/opencga-storage-core/src/main/resources/solr_schema.xml
 *
 <field name="variantId" type="string" indexed="false" stored="true" multiValued="false"/>
 <field name="chromosome" type="string" indexed="true" stored="true" multiValued="false"/>
 <field name="start" type="int" indexed="true" stored="true" multiValued="false"/>
 <field name="end" type="int" indexed="true" stored="true" multiValued="false"/>
 <field name="xrefs" type="string" indexed="true" stored="true" multiValued="true"/>
 <field name="type" type="string" indexed="true" stored="true" multiValued="false"/>
 <field name="studies" type="string" indexed="true" stored="true" multiValued="true"/>
 <field name="phastCons" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="phylop" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="gerp" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="caddRaw" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="caddScaled" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="sift" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="polyphen" type="double" indexed="true" stored="true" multiValued="false"/>
 <field name="genes" type="string" indexed="false" stored="true" multiValued="true"/>
 <field name="biotypes" type="string" indexed="true" stored="true" multiValued="true"/>
 <field name="soAcc" type="int" indexed="true" stored="true" multiValued="true"/>
 <field name="geneToSoAcc" type="string" indexed="true" stored="true" multiValued="true"/>
 <field name="traits" type="text_en" indexed="true" stored="true" multiValued="true"/>
 <dynamicField name="stats_*" type="double" indexed="true" stored="true" multiValued="false"/>
 <dynamicField name="popFreq_*" type="double" indexed="true" stored="true" multiValued="false"/>
 <dynamicField name="attr_s_*" type="string" indexed="true" stored="true" multiValued="false"/>
 <dynamicField name="attr_i_*" type="int" indexed="true" stored="true" multiValued="false"/>
 <dynamicField name="attr_d_*" type="double" indexed="true" stored="true" multiValued="false"/>
 */

public class VariantSearchModel {

    @Field
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

    @Field("traits")
    private List<String> traits;

    @Field("other")
    private List<String> other;

    @Field("stats_*")
    private Map<String, Float> stats;

    @Field("popFreq_*")
    private Map<String, Float> popFreq;

    @Field("attr_s_*")
    private Map<String, String> sAttrs;

    @Field("attr_i_*")
    private Map<String, Integer> iAttrs;

    @Field("attr_d_*")
    private Map<String, Double> dAttrs;

    public static final double MISSING_VALUE = -100.0;

    public VariantSearchModel() {
        phastCons = MISSING_VALUE;
        phylop = MISSING_VALUE;
        gerp = MISSING_VALUE;
        caddRaw = MISSING_VALUE;
        caddScaled = MISSING_VALUE;
        sift = MISSING_VALUE;
        polyphen = MISSING_VALUE;

        this.genes = new ArrayList<>();
        this.soAcc = new ArrayList<>();
        this.geneToSoAcc = new ArrayList<>();
        this.other = new ArrayList<>();
        this.popFreq = new HashMap<>();
        this.sAttrs = new HashMap<>();
        this.iAttrs = new HashMap<>();
        this.dAttrs = new HashMap<>();
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
        sb.append(", traits=").append(traits);
        sb.append(", other=").append(other);
        sb.append(", stats=").append(stats);
        sb.append(", popFreq=").append(popFreq);
        sb.append(", sAttrs=").append(sAttrs);
        sb.append(", iAttrs=").append(iAttrs);
        sb.append(", dAttrs=").append(dAttrs);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
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

    public Map<String, Float> getStats() {
        return stats;
    }

    public VariantSearchModel setStats(Map<String, Float> stats) {
        this.stats = stats;
        return this;
    }

    public Map<String, Float> getPopFreq() {
        return popFreq;
    }

    public VariantSearchModel setPopFreq(Map<String, Float> popFreq) {
        this.popFreq = popFreq;
        return this;
    }

    public Map<String, String> getsAttrs() {
        return sAttrs;
    }

    public VariantSearchModel setsAttrs(Map<String, String> sAttrs) {
        this.sAttrs = sAttrs;
        return this;
    }

    public Map<String, Integer> getiAttrs() {
        return iAttrs;
    }

    public VariantSearchModel setiAttrs(Map<String, Integer> iAttrs) {
        this.iAttrs = iAttrs;
        return this;
    }

    public Map<String, Double> getdAttrs() {
        return dAttrs;
    }

    public VariantSearchModel setdAttrs(Map<String, Double> dAttrs) {
        this.dAttrs = dAttrs;
        return this;
    }
}
