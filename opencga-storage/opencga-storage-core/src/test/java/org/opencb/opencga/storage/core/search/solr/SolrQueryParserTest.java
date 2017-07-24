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

package org.opencb.opencga.storage.core.search.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created by jtarraga on 03/03/17.
 */
public class SolrQueryParserTest {

    public String host = "http://bioinfo.hpc.cam.ac.uk/solr/"; //hgvav1_hgvauser_reference_grch37/select?facet=on&fq=chromosome:22&indent=on&q=*:*&rows=0&wt=json&facet.field=studies&facet.field=type

    //public String collection = "test1";
    public String mode = "cloud";
    public String collection = "hgvav1_hgvauser_reference_grch37";

    public String study = collection;


    public void executeQuery(Query query, QueryOptions queryOptions) {
        String user = "";
        String password = "";
        boolean active = true;
        int timeout = 30000;
        int rows = 10;
        StorageConfiguration config = new StorageConfiguration();
        config.setSearch(new SearchConfiguration(host, mode, user, password, active, timeout, rows));
        VariantSearchManager searchManager = new VariantSearchManager(null, null, config);
        try {
            VariantIterator iterator = searchManager.iterator(collection, query, queryOptions);
            System.out.println("Num. found = " + iterator.getNumFound());
            while (iterator.hasNext()) {
                Variant variant = iterator.next();
                System.out.println(variant.toJson());
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void display(Query query, QueryOptions queryOptions, SolrQuery solrQuery) {
        System.out.println("Query        : " + query.toJson());
        System.out.println("Query options: " + queryOptions.toJson());
        System.out.println("Solr query   : " + solrQuery.toString());
        System.out.println();
    }

    public void parseXref() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_XREF.key(), "rs574335987");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=xrefs:\"rs574335987\"".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }


    public void parseConsequenceTypeSOTerm() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=soAcc:1583".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseConsequenceTypeSOAcc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792,SO:0001619");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=soAcc:1792+OR+soAcc:1619".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }


    public void parseGeneAndConsequenceType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(GENE.key(), "WASH7P");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseRegion() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(REGION.key(), "1:17700");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(chromosome:1+AND+start:17700)".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(TYPE.key(), "CNV,SNV");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(type:\"CNV\"+OR+type:\"SNV\")".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parsePhylop() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_CONSERVATION.key(), "phylop<-1.0");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=phylop:{-100.0+TO+-1.0}".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parsePhylopAndGene() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(GENE.key(), "WASH7P");
        query.put(ANNOT_CONSERVATION.key(), "phylop<-1.0");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=xrefs:\"WASH7P\"&fq=phylop:{-100.0+TO+-1.0}".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parsePhylopAndGeneAndPop() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(GENE.key(), "WASH7P");
        query.put(ANNOT_CONSERVATION.key(), "phylop<-1.0");
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:ALL>0.1");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=xrefs:\"WASH7P\"&fq=phylop:{-100.0+TO+-1.0}&fq=popFreq__1kG_phase3__ALL:{0.1+TO+*]".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseSiftScore() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==tolerated,polyphen==bening");
        //query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift>0.5,polyphen==bening");
        //query.put(ANNOT_SIFT.key(), "tolerated");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(siftDesc:\"tolerated\"+OR+polyphenDesc:\"bening\")".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parsePopMafScore() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:YRI<0.01");
//        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:ALL<0.0002");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=popFreq__1kG_phase3__YRI:{-100.0+TO+0.01}".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parsePopMafScoreMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        // (* -popFreq__1kG_phase3__YRI:*) OR popFreq_1kG_phase3__YRI:[0.01 TO *]
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:YRI<<0.01");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=((*+-popFreq__1kG_phase3__YRI:*)+OR+popFreq__1kG_phase3__YRI:[0+TO+0.01})".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parsePhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_CONSERVATION.key(), "phastCons>0.02");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=phastCons:{0.02+TO+*]".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parsePhastConsMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_CONSERVATION.key(), "phastCons>>0.02");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(phastCons:{0.02+TO+*]+OR+phastCons:\\-100.0)".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseSiftMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<<0.01");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=sift:[-100.0+TO+0.01}".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseNoPhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_CONSERVATION.key(), "phastCons!=0.035999998450279236");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=-phastCons:0.035999998450279236".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseExactPhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_CONSERVATION.key(), "phastCons=0.035999998450279236");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=phastCons:0.035999998450279236".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseNoPopMaf() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:GWD!=0.061946902");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=-popFreq__1kG_phase3__GWD:0.061946902".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }


    public void parseExactPopMaf() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:GWD==0.061946902");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=popFreq__1kG_phase3__GWD:0.061946902".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseExactSiftDesc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==tolerated");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=siftDesc:\"tolerated\"".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseExactSiftDesc2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=tolerated");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=siftDesc:\"tolerated\"".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseNoExactSiftDesc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift!=tolerated");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseExactSift() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==-0.3");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=sift:\\-0.3".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseExactSift2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=-0.3");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=sift:\\-0.3".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseNoExactSift() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift!=-0.3");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=-sift:\\-0.3".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseRegionChromosome() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(REGION.key(), "1,3");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(chromosome:1)+OR+(chromosome:3)".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseRegionChromosomeStart() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(REGION.key(), "1:66381,1:98769");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(chromosome:1+AND+start:66381)+OR+(chromosome:1+AND+start:98769)".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseRegionChromosomeStartEnd() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        query.put(REGION.key(), "1:66381-76381,1:98766-117987");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(chromosome:1+AND+start:[66381+TO+*]+AND+end:[*+TO+76381])+OR+(chromosome:1+AND+start:[98766+TO+*]+AND+end:[*+TO+117987])".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseAnnot() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);
        //query.put(REGION.key(), "1,2");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");
        query.put(ANNOT_XREF.key(), "RIPK2,NCF4");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=geneToSoAcc:RIPK2_1583+OR+geneToSoAcc:NCF4_1583".equals(solrQuery.toString()));

        // execute
//        executeQuery(query, queryOptions);
    }

    public void parseAnnotCT1() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);

        // consequence types and genes
        // no xrefs or regions: genes AND cts
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,coding_sequence_variant");
        query.put(ANNOT_XREF.key(), "RIPK2,NCF4");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=geneToSoAcc:RIPK2_1583+OR+geneToSoAcc:RIPK2_1580+OR+geneToSoAcc:NCF4_1583+OR+geneToSoAcc:NCF4_1580".equals(solrQuery.toString()));

        // execute
        //executeQuery(query, queryOptions);
    }

    public void parseAnnotCT2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);

        // consequence types and genes and xrefs/regions
        // otherwise: [((xrefs OR regions) AND cts) OR (genes AND cts)]

        query.put(REGION.key(), "1,2");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,coding_sequence_variant");
        query.put(ANNOT_XREF.key(), "RIPK2,NCF4");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(((chromosome:1)+OR+(chromosome:2))+AND+(soAcc:1583+OR+soAcc:1580))+OR+(geneToSoAcc:RIPK2_1583+OR+geneToSoAcc:RIPK2_1580+OR+geneToSoAcc:NCF4_1583+OR+geneToSoAcc:NCF4_1580)".equals(solrQuery.toString()));

        //execute
        //executeQuery(query, queryOptions);
    }


    public void parseAnnotCT3() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);

        // consequence types but no genes: (xrefs OR regions) AND cts
        // in this case, the resulting string will never be null, because there are some consequence types!!
        query.put(REGION.key(), "1,2");
        query.put(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant,coding_sequence_variant");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=((chromosome:1)+OR+(chromosome:2))+AND+(soAcc:1583+OR+soAcc:1580)".equals(solrQuery.toString()));

        // execute
        //executeQuery(query, queryOptions);
    }

    public void parseAnnotCT4() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);

        // no consequence types: (xrefs OR regions) but we must add "OR genes", i.e.: xrefs OR regions OR genes
        // we must make an OR with xrefs, genes and regions and add it to the "AND" filter list
        query.put(REGION.key(), "1,2");
        query.put(ANNOT_XREF.key(), "RIPK2,NCF4");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=xrefs:\"RIPK2\"+OR+xrefs:\"NCF4\"+OR+(chromosome:1)+OR+(chromosome:2)".equals(solrQuery.toString()));

        // execute
        //executeQuery(query, queryOptions);
    }

    public void parseTraits() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);

        query.put(ANNOT_TRAITS.key(), "melanoma,recessive");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(traits:\"melanoma\"+OR+traits:\"recessive\")".equals(solrQuery.toString()));

        // execute
        //executeQuery(query, queryOptions);
    }

    public void parseHPOs() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);

        query.put(ANNOT_HPO.key(), "HP%3A000365,HP%3A0000007");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=(traits:\"HP%253A000365\"+OR+traits:\"HP%253A0000007\")".equals(solrQuery.toString()));

        // execute
        //executeQuery(query, queryOptions);
    }

    public void parseClinVars() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        //query.put(STUDIES.key(), study);

        query.put(ANNOT_CLINVAR.key(), "RCV000010071");

        SolrQuery solrQuery = new SolrQueryParser(null, null).parse(query, queryOptions);
        display(query, queryOptions, solrQuery);
        assert("q=*:*&fq=xrefs:\"RCV000010071\"&fq=traits:\"RCV000010071\"".equals(solrQuery.toString()));

        // execute
        //executeQuery(query, queryOptions);
    }



    @Test
    public void testParsing() {
        QueryOptions queryOptions = new QueryOptions();
        Query query = new Query();
//        executeQuery(query, queryOptions);

        parseAnnot();
        parseAnnotCT1();
        parseAnnotCT2();
        parseAnnotCT3();
        parseAnnotCT4();

        parseSiftMissing();

        parsePhastCons();
        parsePhastConsMissing();

        parsePopMafScore();
        parsePopMafScoreMissing();

        parseConsequenceTypeSOAcc();
        parseConsequenceTypeSOTerm();

        parseNoPhastCons();
        parseExactPhastCons();

        parseNoPopMaf();
        parseExactPopMaf();

        parseExactSiftDesc();
        parseExactSiftDesc2();
        parseNoExactSiftDesc();

        parseExactSift();
        parseExactSift2();
        parseNoExactSift();

        parseRegionChromosome();
        parseRegionChromosomeStart();
        parseRegionChromosomeStartEnd();

        parseSiftScore();

        parseXref();
        parsePhylop();
        parseConsequenceTypeSOAcc();
        parseConsequenceTypeSOTerm();
        parseRegion();
        parseType();
        parsePhylop();
        parsePhylopAndGene();
        parsePhylopAndGeneAndPop();

        parseTraits();
        parseHPOs();
        parseClinVars();

    }
}
