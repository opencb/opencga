package org.opencb.opencga.storage.core.search.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.search.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

/**
 * Created by jtarraga on 03/03/17.
 */
public class ParseSolrQueryTest {

    public String collection = "test1";
    public String study = "test1";

    public void executeQuery(Query query, QueryOptions queryOptions) {
        SolrQuery solrQuery = ParseSolrQuery.parse(query, queryOptions);

        String host = "http://localhost:8983/solr/";
        String user = "";
        String password = "";
        boolean active = true;
        int rows = 10;
        StorageConfiguration config = new StorageConfiguration();
        config.setSearch(new SearchConfiguration(host, collection, user, password, active, rows));
        VariantSearchManager searchManager = new VariantSearchManager(config);
        try {
            SolrVariantIterator iterator = searchManager.iterator(collection, query, queryOptions);
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
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_XREF.key(), "rs574335987");

        // execute
        executeQuery(query, queryOptions);
    }


    public void parseConsequenceTypeSOTerm() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parseConsequenceTypeSOAcc() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792,SO:0001619");

        // execute
        executeQuery(query, queryOptions);
    }


    public void parseGeneAndConsequenceType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), "WASH7P");
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), "SO:0001792");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parseRegion() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), "1:17700");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parseType() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.TYPE.key(), "CNV,SNV");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhylop() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), "phylop<-1.0");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhylopAndGene() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), "WASH7P");
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), "phylop<-1.0");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhylopAndGeneAndPop() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), "WASH7P");
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), "phylop<-1.0");
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:ALL>0.1");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parseSiftScore() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift==tolerated,polyphen==bening");
        //query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift>0.5,polyphen==bening");
        //query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), "tolerated");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePopMafScore() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:YRI<0.01");
//        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:ALL<0.0002");

        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePopMafScoreMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        // (* -popFreq__1kG_phase3__YRI:*) OR popFreq_1kG_phase3__YRI:[0.01 TO *]
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), "1kG_phase3:YRI<<0.01");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhastCons() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), "phastCons>0.02");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parsePhastConsMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), "phastCons>>0.02");
        // execute
        executeQuery(query, queryOptions);
    }

    public void parseSiftMissing() {
        QueryOptions queryOptions = new QueryOptions();

        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study);
        query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<<0.01");
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
