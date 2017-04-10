package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.commons.datastore.core.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created on 10/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MultiQueryGenerator extends QueryGenerator {

    public static final String MULTI_QUERY = "multy-query";
    private List<QueryGenerator> generators;
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
        String query = params.get(MULTI_QUERY);
        generators = new ArrayList<>();
        for (String param : query.split(",")) {
            QueryGenerator queryGenerator;
            switch (param) {
                case "region":
                    queryGenerator = new RegionQueryGenerator();
                    break;
                case "gene":
                    queryGenerator = new TermQueryGenerator.GeneQueryGenerator();
                    break;
                case "ct":
                    queryGenerator = new TermQueryGenerator.ConsequenceTypeQueryGenerator();
                    break;
                case "type":
                    queryGenerator = new TermQueryGenerator.TypeQueryGenerator();
                    break;
                case "study":
                case "studies":
                    queryGenerator = new TermQueryGenerator.StudyQueryGenerator();
                    break;
                case "biotype":
                    queryGenerator = new TermQueryGenerator.BiotypeQueryGenerator();
                    break;
                case "xrefs":
                    queryGenerator = new TermQueryGenerator.XrefQueryGenerator();
                    break;
                case "conservation":
                    queryGenerator = new ScoreQueryGenerator.ConservationQueryGenerator();
                    break;
                case "protein-substitution":
                    queryGenerator = new ScoreQueryGenerator.ProteinSubstQueryGenerator();
                    break;
                case "functional":
                    queryGenerator = new ScoreQueryGenerator.FunctionalScoreQueryGenerator();
                    break;
                default:
                    throw new IllegalArgumentException("Unknwon query param " + param);
            }
            logger.debug("Using sub query generator: " + queryGenerator.getClass());
            queryGenerator.setUp(params);
            generators.add(queryGenerator);
        }
    }

    @Override
    public Query generateQuery(Query query) {
        for (QueryGenerator generator : generators) {
            generator.generateQuery(query);
        }
        return query;
    }
}
