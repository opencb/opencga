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

package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created by jtarraga on 03/03/17.
 */
public class SolrQueryParserTest {

    private String studyName = "platinum";
    private String flBase = "fl=other,geneToSoAcc,traits,type,soAcc,score_*,sift,caddRaw,biotypes,polyphenDesc,studies,end,id,variantId,"
            + "popFreq_*,caddScaled,genes,stats_*,chromosome,xrefs,start,gerp,polyphen,siftDesc,"
            + "phastCons,phylop,id,chromosome,start,end,type";
//    private String flDefault1 = flBase + ",fileInfo__*,qual__*,filter__*,sampleFormat__*__format,sampleFormat__*";
    private String flDefault1 = flBase + ",fileInfo__*,qual__*,filter__*,sampleFormat__*";
    private String flDefaultStudy = flBase + ",fileInfo__" + studyName + "__*,qual__" + studyName + "__*,"
            + "filter__" + studyName + "__*,sampleFormat__" + studyName + "__*";

    SolrQueryParser solrQueryParser;

    VariantStorageMetadataManager scm;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() throws StorageEngineException {
        DummyVariantStorageMetadataDBAdaptorFactory.clear();
        scm = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        scm.createStudy(studyName);

        solrQueryParser = new SolrQueryParser(scm);
    }

    @Test
    public void parseXref() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_XREF.key(), "rs574335987");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=xrefs:\"rs574335987\"", solrQuery.toString());
    }

    @Test
    public void parseConsequenceTypeSOTerm() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(soAcc:\"1583\")", solrQuery.toString());
    }

    @Test
    public void parseConsequenceTypeSOAcc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792,SO:0001619");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(soAcc:\"1792\"+OR+soAcc:\"1619\")", solrQuery.toString());
    }

    @Test
    public void parseGeneAndConsequenceType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(GENE.key(), "WASH7P");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(geneToSoAcc:\"WASH7P_1792\")", solrQuery.toString());
    }

    @Test
    public void parseRegion() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(REGION.key(), "1:17700");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(chromosome:\"1\"+AND+end:[17700+TO+*]+AND+start:[*+TO+17700])", solrQuery.toString());
    }

    @Test
    public void parseGene() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(GENE.key(), "WASH7P");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=xrefs:\"WASH7P\"", solrQuery.toString());
    }

    @Test
    public void parseBiotype() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_BIOTYPE.key(), "protein_coding");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(biotypes:\"protein_coding\")", solrQuery.toString());
    }

    @Test
    public void parseGeneAndConsequenceTypeAndRegionAndBiotype() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(GENE.key(), "WASH7P");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792");
        query.put(REGION.key(), "1:17700");
        query.put(ANNOT_BIOTYPE.key(), "protein_coding");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(((chromosome:\"1\"+AND+end:[17700+TO+*]+AND+start:[*+TO+17700]))+AND+(geneToSoAcc:\"protein_coding_1792\"))+OR+(geneToSoAcc:\"WASH7P_protein_coding_1792\")", solrQuery.toString());
    }

    @Test
    public void parseType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(TYPE.key(), "CNV,SNV");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(type:\"CNV\"+OR+type:\"SNV\")", solrQuery.toString());
    }

    @Test
    public void parsePhylop() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_CONSERVATION.key(), "phylop<-1.0");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=phylop:{-100.0+TO+-1.0}", solrQuery.toString());
    }

    @Test
    public void parsePhylopAndGene() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(GENE.key(), "WASH7P");
        query.put(ANNOT_CONSERVATION.key(), "phylop<-1.0");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=xrefs:\"WASH7P\"&fq=phylop:{-100.0+TO+-1.0}", solrQuery.toString());
    }

    @Test
    public void parsePhylopAndBiotype() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_BIOTYPE.key(), "protein_coding");
        query.put(ANNOT_CONSERVATION.key(), "phylop<-1.0");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(biotypes:\"protein_coding\")&fq=phylop:{-100.0+TO+-1.0}", solrQuery.toString());
    }

    @Test
    public void parsePhylopAndGeneAndPop() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(GENE.key(), "WASH7P");
        query.put(ANNOT_CONSERVATION.key(), "phylop<-1.0");
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:ALL>0.1");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=xrefs:\"WASH7P\"&fq=phylop:{-100.0+TO+-1.0}&fq=popFreq__1kG_phase3__ALL:{0.1+TO+*]", solrQuery.toString());
    }

    @Test
    public void parseStats() throws StorageEngineException {
        QueryOptions queryOptions = new QueryOptions();
        Query query = new Query();
        query.put(STATS_MAF.key(), "ALL<0.1");
        String expectedFilter = "&q=*:*&fq=(stats__platinum__ALL:[0+TO+0.1})";

        // Without study
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        assertEquals(flDefault1 + expectedFilter, solrQuery.toString());

        // With study
        query.put(STUDY.key(), studyName);
        solrQuery = solrQueryParser.parse(query, queryOptions);
        assertEquals(flDefaultStudy + expectedFilter, solrQuery.toString());

        // With study in stats
        query.put(STATS_MAF.key(), studyName + ":ALL<0.1");
        solrQuery = solrQueryParser.parse(query, queryOptions);
        assertEquals(flDefaultStudy + expectedFilter, solrQuery.toString());

        // Without study : FAIL
        scm.createStudy("secondStudy");
        query.remove(STUDY.key());
        query.put(STATS_MAF.key(), "ALL<0.1");
        thrown.expect(VariantQueryException.class);
        thrown.expectMessage("Missing study");
        solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
    }

    @Test
    public void parseMultiStats() {
        QueryOptions queryOptions = new QueryOptions();
        Query query = new Query();
        query.put(STATS_MAF.key(), "ALL<0.1;OTH<0.1");
        String expectedFilter = "&q=*:*&fq="
                + "((stats__platinum__ALL:[0+TO+0.1})"
                + "+AND+"
                + "(stats__platinum__OTH:[0+TO+0.1}))";

        // Without study
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        assertEquals(flDefault1 + expectedFilter, solrQuery.toString());

        // With study
        query.put(STUDY.key(), studyName);
        solrQuery = solrQueryParser.parse(query, queryOptions);
        assertEquals(flDefaultStudy + expectedFilter, solrQuery.toString());

        // With study in some stats
        query.put(STATS_MAF.key(), studyName + ":ALL<0.1;OTH<0.1");
        solrQuery = solrQueryParser.parse(query, queryOptions);
        assertEquals(flDefaultStudy + expectedFilter, solrQuery.toString());

        // With study in stats
        query.put(STATS_MAF.key(), studyName + ":ALL<0.1;" + studyName + ":OTH<0.1");
        solrQuery = solrQueryParser.parse(query, queryOptions);
        assertEquals(flDefaultStudy + expectedFilter, solrQuery.toString());
    }

    @Test
    public void parseProteinSubstitutionScore() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==tolerated,polyphen==bening");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(siftDesc:\"tolerated\"+OR+polyphenDesc:\"bening\")", solrQuery.toString());


        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen==possibly damaging,probably damaging");

        solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(polyphenDesc:\"possibly+damaging\"+OR+polyphenDesc:\"probably+damaging\")", solrQuery.toString());
    }

    @Test
    public void parsePopMafScore() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:YRI<0.01");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(popFreq__1kG_phase3__YRI:[0+TO+0.01}+OR+(*+-popFreq__1kG_phase3__YRI:*))", solrQuery.toString());
    }

    @Test
    public void parsePopMafScoreMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        // (* -popFreq__1kG_phase3__YRI:*) OR popFreq_1kG_phase3__YRI:[0.01 TO *]
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:YRI<<0.01");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(popFreq__1kG_phase3__YRI:[0+TO+0.01}+OR+(*+-popFreq__1kG_phase3__YRI:*))", solrQuery.toString());
    }

    @Test
    public void parsePhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_CONSERVATION.key(), "phastCons>0.02");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=phastCons:{0.02+TO+*]", solrQuery.toString());
    }

    @Test
    public void parsePhastConsMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_CONSERVATION.key(), "phastCons>>0.02");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(phastCons:{0.02+TO+*]+OR+phastCons:\\-100.0)", solrQuery.toString());
    }

    @Test
    public void parseSiftMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<<0.01");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=sift:[-100.0+TO+0.01}", solrQuery.toString());
    }

    @Test
    public void parseNoPhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_CONSERVATION.key(), "phastCons!=0.035999998450279236");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=-phastCons:0.035999998450279236", solrQuery.toString());
    }

    @Test
    public void parseExactPhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_CONSERVATION.key(), "phastCons=0.035999998450279236");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=phastCons:0.035999998450279236", solrQuery.toString());
    }

    @Test
    public void parseNoPopMaf() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:GWD!=0.061946902");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=-popFreq__1kG_phase3__GWD:0.061946902", solrQuery.toString());
    }

    @Test
    public void parseExactPopMaf() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:GWD==0.061946902");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=popFreq__1kG_phase3__GWD:0.061946902", solrQuery.toString());
    }

    @Test
    public void parseExactSiftDesc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==tolerated");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=siftDesc:\"tolerated\"", solrQuery.toString());
    }

    @Test
    public void parseExactSiftDesc2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=tolerated");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=siftDesc:\"tolerated\"", solrQuery.toString());
    }

    @Test
    public void parseNoExactSiftDesc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift!=tolerated");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=-siftDesc:\"tolerated\"", solrQuery.toString());
    }

    @Test
    public void parseExactSift() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==-0.3");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=sift:\\-0.3", solrQuery.toString());
    }

    @Test
    public void parseExactSift2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=-0.3");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=sift:\\-0.3", solrQuery.toString());
    }

    @Test
    public void parseNoExactSift() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift!=-0.3");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=-sift:\\-0.3", solrQuery.toString());
    }

    @Test
    public void parseRegionChromosome() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(REGION.key(), "1,3");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(chromosome:\"1\")+OR+(chromosome:\"3\")", solrQuery.toString());
    }

    @Test
    public void parseRegionChromosomeStart() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(REGION.key(), "1:66381,1:98769");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(chromosome:\"1\"+AND+end:[66381+TO+*]+AND+start:[*+TO+66381])+OR+(chromosome:\"1\"+AND+end:[98769+TO+*]+AND+start:[*+TO+98769])", solrQuery.toString());
    }

    @Test
    public void parseRegionChromosomeStartEnd() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(REGION.key(), "1:66381-76381,1:98766-117987");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(chromosome:\"1\"+AND+end:[66381+TO+*]+AND+start:[*+TO+76381])+OR+(chromosome:\"1\"+AND+end:[98766+TO+*]+AND+start:[*+TO+117987])", solrQuery.toString());
    }

    @Test
    public void parseAnnot() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(ANNOT_XREF.key(), "RIPK2,NCF4");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(geneToSoAcc:\"RIPK2_1583\"+OR+geneToSoAcc:\"NCF4_1583\")", solrQuery.toString());
    }

    @Test
    public void parseAnnotCT1() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();

        // consequence types and genes
        // no xrefs or regions: genes AND cts
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,coding_sequence_variant");
        query.put(ANNOT_XREF.key(), "RIPK2,NCF4");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(geneToSoAcc:\"RIPK2_1583\"+OR+geneToSoAcc:\"RIPK2_1580\"+OR+geneToSoAcc:\"NCF4_1583\"+OR+geneToSoAcc:\"NCF4_1580\")", solrQuery.toString());
    }

    @Test
    public void parseAnnotCT2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();

        // consequence types and genes and xrefs/regions
        // otherwise: [((xrefs OR regions) AND cts) OR (genes AND cts)]

        query.put(REGION.key(), "1,2");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,coding_sequence_variant");
        query.put(ANNOT_XREF.key(), "RIPK2,NCF4");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(((chromosome:\"1\")+OR+(chromosome:\"2\"))+AND+(soAcc:\"1583\"+OR+soAcc:\"1580\"))+OR+(geneToSoAcc:\"RIPK2_1583\"+OR+geneToSoAcc:\"RIPK2_1580\"+OR+geneToSoAcc:\"NCF4_1583\"+OR+geneToSoAcc:\"NCF4_1580\")", solrQuery.toString());
    }

    @Test
    public void parseAnnotCT3() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);

        // consequence types but no genes: (xrefs OR regions) AND cts
        // in this case, the resulting string will never be null, because there are some consequence types!!
        query.put(REGION.key(), "1,2");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,coding_sequence_variant");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(((chromosome:\"1\")+OR+(chromosome:\"2\"))+AND+(soAcc:\"1583\"+OR+soAcc:\"1580\"))", solrQuery.toString());
    }

    @Test
    public void parseAnnotCT4() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();

        // no consequence types: (xrefs OR regions) but we must add "OR genes", i.e.: xrefs OR regions OR genes
        // we must make an OR with xrefs, genes and regions and add it to the "AND" filter list
        query.put(REGION.key(), "1,2");
        query.put(ANNOT_XREF.key(), "RIPK2,NCF4");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=xrefs:\"RIPK2\"+OR+xrefs:\"NCF4\"+OR+(chromosome:\"1\")+OR+(chromosome:\"2\")", solrQuery.toString());
    }

    @Test
    public void parseTraits() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();

        query.put(ANNOT_TRAIT.key(), "melanoma,recessive");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(traits:\"melanoma\"+OR+traits:\"recessive\")", solrQuery.toString());
    }

    @Test
    public void parseHPOs() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();

        query.put(ANNOT_HPO.key(), "HP%3A000365,HP%3A0000007");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=(traits:\"HP%253A000365\"+OR+traits:\"HP%253A0000007\")", solrQuery.toString());
    }

    @Test
    public void parseFormat() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.EXCLUDE, VariantField.STUDIES_FILES + "," + VariantField.STUDIES_SAMPLES_DATA);

        Query query = new Query();
        query.put(STUDY.key(), studyName);

        query.put(FORMAT.key(), "NA12877:DP>300;NA12878:DP>500");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flBase + "&q=*:*&fq=(dp__" + studyName + "__NA12877:{300+TO+*]+AND+dp__" + studyName + "__NA12878:{500+TO+*])", solrQuery.toString());
    }

    @Test
    public void parseWrongFormat() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(STUDY.key(), studyName);

        query.put(FORMAT.key(), "NA12877:AC>200");

        try {
            solrQueryParser.parse(query, queryOptions);
        } catch (VariantQueryException e) {
            System.out.println("Success, exception caught!! " + e.getMessage());
            return;
        }
        fail();
    }

    @Test
    public void parseSample() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.EXCLUDE, VariantField.STUDIES_FILES);

        Query query = new Query();
        query.put(STUDY.key(), studyName);

        query.put(GENOTYPE.key(), "NA12877:1/0,NA12878:1/1");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);

        String fl = ",sampleFormat__platinum__sampleName,sampleFormat__platinum__format,sampleFormat__platinum__NA12877,sampleFormat__platinum__NA12878";
        assertEquals(flBase + fl + "&q=*:*&fq=((gt__" + studyName + "__NA12878:\"1/1\")+OR+(gt__" + studyName + "__NA12877:\"1/0\"))", solrQuery.toString());
    }

    @Test
    public void parseVariantScore1() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(SCORE.key(), studyName + ":score1<0.01");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=score__platinum__score1:{-100.0+TO+0.01}", solrQuery.toString());
    }

    @Test
    public void parseVariantScore2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(SCORE.key(), studyName + ":score2>=3.2;" + studyName + ":score3:pvalue<0.02");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=score__platinum__score2:[3.2+TO+*]&fq=scorePValue__platinum__score3:{-100.0+TO+0.02}", solrQuery.toString());
    }

    @Test
    public void parseVariantScoreWithoutStudy() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(SCORE.key(), "score4>=3.2,score2:pvalue<0.001");

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals(flDefault1 + "&q=*:*&fq=score__platinum__score4:[3.2+TO+*]+OR+scorePValue__platinum__score2:{-100.0+TO+0.001}", solrQuery.toString());
    }


    @Test
    public void parseFacetAvgProteinSubst() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.FACET, "avg(" + ANNOT_PROTEIN_SUBSTITUTION.key() + "[sift])");

        Query query = new Query();

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals("json.facet={\"sift___avg___0\":\"avg(sift)\"}&rows=0&start=0&q=*:*", solrQuery.toString());
    }

    @Test
    public void parseFacetRangeProteinSubst() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.FACET, ANNOT_PROTEIN_SUBSTITUTION.key() + "[sift:0..100]:5");

        Query query = new Query();

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals("json.facet={\"sift___0___100___5___0\":{\"field\":\"sift\",\"start\":0,\"end\":100,\"type\":\"range\",\"gap\":5}}&rows=0&start=0&q=*:*", solrQuery.toString());
    }

    @Test
    public void parseFacetRangePopFreqAlt() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.FACET, ANNOT_POPULATION_ALTERNATE_FREQUENCY.key() + "[1kG_phase3:CEU:0..1]:0.2");

        Query query = new Query();

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals("json.facet={\"popFreq__1kG_phase3__CEU___0___1___0.2___0\":{\"field\":\"popFreq__1kG_phase3__CEU\",\"start\":0,\"end\":1,\"type\":\"range\",\"gap\":0.2}}&rows=0&start=0&q=*:*", solrQuery.toString());
    }

    @Test
    public void parseFacetAvgPopFreqAlt() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.FACET, "avg(" + ANNOT_POPULATION_ALTERNATE_FREQUENCY.key() + "[1kG_phase3:CEU])");

        Query query = new Query();

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals("json.facet={\"popFreq__1kG_phase3__CEU___avg___0\":\"avg(popFreq__1kG_phase3__CEU)\"}&rows=0&start=0&q=*:*", solrQuery.toString());
    }

    @Test
    public void parseFacetPercentilePopFreqAlt() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.FACET, "percentile(" + ANNOT_POPULATION_ALTERNATE_FREQUENCY.key() + "[1kG_phase3:CEU])");

        Query query = new Query();

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals("json.facet={\"popFreq__1kG_phase3__CEU___percentile___0\":\"percentile(popFreq__1kG_phase3__CEU,1,10,25,50,75,90,99)\"}&rows=0&start=0&q=*:*", solrQuery.toString());
    }

    @Test
    public void parseFacetRangeCohortStatsFreqAlt() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.FACET, STATS_ALT.key() + "[1kG_phase3:CEU:0..1]:0.2");

        Query query = new Query();

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals("json.facet={\"stats__1kG_phase3__CEU___0___1___0.2___0\":{\"field\":\"stats__1kG_phase3__CEU\",\"start\":0,\"end\":1,\"type\":\"range\",\"gap\":0.2}}&rows=0&start=0&q=*:*", solrQuery.toString());
    }

    @Test
    public void parseFacetRangeScore() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.FACET, SCORE.key() + "[1kG_phase3:score2:0..1]:0.2");

        Query query = new Query();

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals("json.facet={\"score__1kG_phase3__score2___0___1___0.2___0\":{\"field\":\"score__1kG_phase3__score2\",\"start\":0,\"end\":1,\"type\":\"range\",\"gap\":0.2}}&rows=0&start=0&q=*:*", solrQuery.toString());
    }

    @Test
    public void parseFacetAvgScore() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(QueryOptions.FACET, "avg(" + SCORE.key() + "[1kG_phase3:score2])");

        Query query = new Query();

        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assertEquals("json.facet={\"score__1kG_phase3__score2___avg___0\":\"avg(score__1kG_phase3__score2)\"}&rows=0&start=0&q=*:*", solrQuery.toString());
    }

    //-------------------------------------------------------------------------

    private void display(Query query, QueryOptions queryOptions, SolrQuery solrQuery) {
        System.out.println("Query        : " + query.toJson());
        System.out.println("Query options: " + queryOptions.toJson());
        System.out.println("Solr query   : " + solrQuery.toString());
        System.out.println();
    }
}
