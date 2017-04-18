package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class RegionQueryGenerator extends QueryGenerator {

    private ArrayList<Region> regionLimits = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
        readCsvFile(Paths.get(params.get(DATA_DIR), "regions.csv"), strings -> {
            if (strings.size() == 3) {
                regionLimits.add(new Region(strings.get(0), Integer.parseInt(strings.get(1)), Integer.parseInt(strings.get(2))));
            } else {
                regionLimits.add(new Region(strings.get(0)));
            }
        });
        regionLimits.trimToSize();
    }

    @Override
    public Query generateQuery(Query query) {
        List<String> regions = new ArrayList<>(getArity());
        for (int i = 0; i < getArity(); i++) {
            Region regionLimit = regionLimits.get(random.nextInt(regionLimits.size()));
            int regionLength = random.nextInt(1000);
            int start = random.nextInt(regionLimit.getEnd() - regionLimit.getStart() - regionLength) + regionLimit.getStart();
            int end = start + regionLength;
            Region region = new Region(regionLimit.getChromosome(), start, end);
            regions.add(region.toString());
        }
        query.append(VariantQueryParam.REGION.key(), String.join(",", regions));
        return query;
    }
}
