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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

/**
 * Created by jtarraga on 03/03/17.
 */
public class ParseSolrQueryTest {

    public String collection = "test444";
    public String study = "test444";

    public void executeQuery(Query query, QueryOptions queryOptions) {
        SolrQueryParser solrQueryParser = new SolrQueryParser(null, null);
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);

        String host = "http://localhost:8983/solr/";
        String user = "";
        String password = "";
        boolean active = true;
        int rows = 10;
        StorageConfiguration config = new StorageConfiguration();
        config.setSearch(new SearchConfiguration(host, collection, user, password, active, 30000, rows));
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

    public void parseXref() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_XREF.key(), "rs574335987");

        // execute
        executeQuery(query, queryOptions);
    }


    public void parseConsequenceTypeSOTerm() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parseConsequenceTypeSOAcc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792,SO:0001619");

        // execute
        executeQuery(query, queryOptions);
    }


    public void parseGeneAndConsequenceType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.GENE.key(), "WASH7P");
        query.put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parseRegion() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.REGION.key(), "1:17700");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parseType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.TYPE.key(), "CNV,SNV");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhylop() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "phylop<-1.0");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhylopAndGene() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.GENE.key(), "WASH7P");
        query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "phylop<-1.0");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhylopAndGeneAndPop() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.GENE.key(), "WASH7P");
        query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "phylop<-1.0");
        query.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:ALL>0.1");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parseSiftScore() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==tolerated,polyphen==bening");
        //query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift>0.5,polyphen==bening");
        //query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), "tolerated");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePopMafScore() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:YRI<0.01");
//        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:ALL<0.0002");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePopMafScoreMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        // (* -popFreq__1kG_phase3__YRI:*) OR popFreq_1kG_phase3__YRI:[0.01 TO *]
        query.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:YRI<<0.01");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "phastCons>0.02");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhastConsMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "phastCons>>0.02");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseSiftMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<<0.01");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseNoPhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "phastCons!=0.035999998450279236");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseExactPhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_CONSERVATION.key(), "phastCons=0.035999998450279236");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseNoPopMaf() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:GWD!=0.061946902");
        // execute
        executeQuery(query, queryOptions);
    }


    public void parseExactPopMaf() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:GWD==0.061946902");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseExactSiftDesc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==tolerated");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseExactSiftDesc2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=tolerated");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseNoExactSiftDesc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift!=tolerated");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseExactSift() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==-0.3");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseExactSift2() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift=-0.3");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseNoExactSift() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift!=-0.3");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseRegionChromosome() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.REGION.key(), "1,3");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseRegionChromosomeStart() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.REGION.key(), "1:66381,1:98769");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseRegionChromosomeStartEnd() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantQueryParam.STUDIES.key(), study);
        query.put(VariantQueryParam.REGION.key(), "1:66381-76381,1:98766-117987");
        // execute
        executeQuery(query, queryOptions);
    }


//    @Test
    public void testParsing() {
        QueryOptions queryOptions = new QueryOptions();
        Query query = new Query();
//        executeQuery(query, queryOptions);

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

/*
        parseXref();
        parsePhylop();
        parseConsequenceTypeSOAcc();
        parseConsequenceTypeSOTerm();
        parseRegion();
        parseType();
        parsePhylop();
        parsePhylopAndGene();
        parsePhylopAndGeneAndPop();
        */
    }
}
