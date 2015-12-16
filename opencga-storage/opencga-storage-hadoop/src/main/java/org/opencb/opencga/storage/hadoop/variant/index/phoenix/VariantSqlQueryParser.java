package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * Created on 16/12/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSqlQueryParser {

    private final GenomeHelper genomeHelper;
    private final String variantTable;

    public VariantSqlQueryParser(GenomeHelper genomeHelper, String variantTable) {
        this.genomeHelper = genomeHelper;
        this.variantTable = variantTable;
    }

    public String parse(Query query, QueryOptions options) {


        StringBuilder sb = new StringBuilder("SELECT ");

        addProjectedColumns(sb, query, options);

        sb.append(" FROM \"").append(variantTable).append("\"");


        List<String> regionFilters = getRegionFilters(query);
        List<String> filters = getOtherFilters(query);

        // Add WHERE
        addWhereStatement(sb, regionFilters, filters);

        if (options.getInt("limit") > 0) {
            sb.append(" LIMIT ").append(options.getInt("limit"));
        }

        return sb.toString();
    }


    public StringBuilder addProjectedColumns(StringBuilder sb, Query query, QueryOptions options) {
        //TODO Fetch only FULL_ANNOTATION and genotypes data
        return sb.append(" * ");
    }

    public StringBuilder addWhereStatement(StringBuilder sb, List<String> regionFilters, List<String> filters) {
        if (!regionFilters.isEmpty() || !filters.isEmpty()) {
            sb.append(" WHERE");
        }

        appendFilters(sb, regionFilters, "OR");

        if (!filters.isEmpty() && !regionFilters.isEmpty()) {
            sb.append(" AND ");
        }

        appendFilters(sb, filters, "AND");

        return sb;
    }

    public StringBuilder appendFilters(StringBuilder sb, List<String> filters, String delimiter) {
        delimiter = " " + delimiter + " ";
        if (!filters.isEmpty()) {
            sb.append(filters.stream().collect(Collectors.joining(delimiter, " ( ", " ) ")));
        }
        return sb;
    }

    public List<String> getRegionFilters(Query query) {
        List<String> regionFilters = new LinkedList<>();
        if (!StringUtils.isEmpty(query.getString(REGION.key()))) {
            List<Region> regions = Region.parseRegions(query.getString(REGION.key()));
            for (Region region : regions) {
                regionFilters.add(Columns.CHROMOSOME + " = '" + region.getChromosome() + "'"
                        + " AND " + Columns.POSITION + " >= " + region.getStart()
                        + " AND " + Columns.POSITION + " <= " + region.getEnd());
            }
        }

        if (!StringUtils.isEmpty(query.getString(GENE.key()))) {
            for (String gene : query.getAsStringList(GENE.key())) {
                regionFilters.add("'" + gene + "' = ANY(" + Columns.GENES + ")");
            }
        }

        if (regionFilters.isEmpty()) {
            // chromosome != _METADATA
            regionFilters.add(CHROMOSOME + " != '" + genomeHelper.getMetaRowKeyString() + "'");
        }
        return regionFilters;
    }

    public List<String> getOtherFilters(Query query) {
        List<String> filters = new LinkedList<>();
        if (!StringUtils.isEmpty(query.getString(ANNOT_BIOTYPE.key()))) {
            for (String biotype : query.getAsStringList(ANNOT_BIOTYPE.key())) {
                filters.add("'" + biotype + "' = ANY(" + Columns.BIOTYPE + ")");
            }
        }

        if (!StringUtils.isEmpty(query.getString(ANNOT_CONSEQUENCE_TYPE.key()))) {
            for (String so : query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key())) {
                //TODO: Catch number format exception
                int soInt = Integer.parseInt(so.toUpperCase().replace("SO:", ""));
                filters.add(soInt + " = ANY(" + Columns.SO + ")");
            }
        }

        if (!StringUtils.isEmpty(query.getString(ANNOTATION_EXISTS.key()))) {
            if (query.getBoolean(ANNOTATION_EXISTS.key())) {
                filters.add(Columns.FULL_ANNOTATION + " IS NOT NULL");
            } else {
                filters.add(Columns.FULL_ANNOTATION + " IS NULL");
            }
        }
        return filters;
    }
}
