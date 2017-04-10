package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GeneQueryGenerator extends TermQueryGenerator {

    //private ArrayList<String> genes = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(getClass());

    public GeneQueryGenerator() {
        super("genes.csv", VariantDBAdaptor.VariantQueryParams.GENE.key());
    }
/*
    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
        readCsvFile(Paths.get(params.get(DATA_DIR), "genes.csv"), strings -> terms.add(strings.get(0)));
        genes.trimToSize();
    }

    @Override
    public Query generateQuery() {
        Query query = new Query();
        query.append(VariantDBAdaptor.VariantQueryParams.GENE.key(), genes.get(random.nextInt(genes.size())));
        return query;
    }
    */
}
