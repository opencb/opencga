package org.opencb.opencga.storage.hadoop.variant.index.phoenix;

import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.Columns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * Created on 16/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSqlQueryParser {

    public static final String COUNT = "count";
    private final GenomeHelper genomeHelper;
    private final String variantTable;
    private final Logger logger = LoggerFactory.getLogger(VariantSqlQueryParser.class);

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


    /**
     * Select only the required columns.
     *
     * Uses the params:
     * {@link VariantQueryParams#RETURNED_STUDIES}
     * {@link VariantQueryParams#RETURNED_SAMPLES}
     * {@link VariantQueryParams#RETURNED_FILES}
     * {@link VariantQueryParams#UNKNOWN_GENOTYPE}
     *
     * @param sb    SQLStringBuilder
     * @param query Query to parse
     * @param options   other options
     * @return String builder
     */
    public StringBuilder addProjectedColumns(StringBuilder sb, Query query, QueryOptions options) {
        if (options.getBoolean(COUNT)) {
            return sb.append(" COUNT(*) ");
        } else {
            //TODO Fetch only FULL_ANNOTATION and genotypes data
            return sb.append(" * ");
        }
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

    /**
     * Transform QueryParams that are inclusive.

     * A variant will pass this filters if matches with ANY of this filters.
     *
     * {@link VariantQueryParams#REGION}
     * {@link VariantQueryParams#CHROMOSOME}
     * {@link VariantQueryParams#REFERENCE}
     * {@link VariantQueryParams#ALTERNATE}
     *
     * Using annotation:
     * {@link VariantQueryParams#ID}
     * {@link VariantQueryParams#GENE}
     *
     * @param query Query to parse
     * @return List of region filters
     */
    public List<String> getRegionFilters(Query query) {
        List<String> regionFilters = new LinkedList<>();


        if (isValidParam(query, REGION)) {
            List<Region> regions = Region.parseRegions(query.getString(REGION.key()));
            for (Region region : regions) {
                regionFilters.add("( " + Columns.CHROMOSOME + " = '" + region.getChromosome() + "'"
                        + " AND " + Columns.POSITION + " >= " + region.getStart()
                        + " AND " + Columns.POSITION + " <= " + region.getEnd() + " )");
            }
        }

        if (isValidParam(query, CHROMOSOME)) {
            for (String chromosome : query.getAsStringList(CHROMOSOME.key())) {
                regionFilters.add(Columns.CHROMOSOME + " = " + chromosome);
            }
        }

        if (isValidParam(query, REFERENCE)) {
            logger.warn("Unsupported filter " +  REFERENCE);
        }

        if (isValidParam(query, ALTERNATE)) {
            logger.warn("Unsupported filter " +  ALTERNATE);
        }

        if (isValidParam(query, ID)) {
            logger.warn("Unsupported filter " +  ID);
        }

        if (isValidParam(query, GENE)) {
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

    /**
     * Transform QueryParams that are exclusive.
     *
     * A variant will pass this filters if matches with ALL of this filters.
     *
     * Variant filters:
     * {@link VariantQueryParams#TYPE}
     * {@link VariantQueryParams#STUDIES}
     * {@link VariantQueryParams#FILES}
     * {@link VariantQueryParams#COHORTS}
     * {@link VariantQueryParams#GENOTYPE}
     *
     * Annotation filters:
     * {@link VariantQueryParams#ANNOTATION_EXISTS}
     * {@link VariantQueryParams#ANNOT_CONSEQUENCE_TYPE}
     * {@link VariantQueryParams#ANNOT_XREF}
     * {@link VariantQueryParams#ANNOT_BIOTYPE}
     * {@link VariantQueryParams#POLYPHEN}
     * {@link VariantQueryParams#SIFT}
     * {@link VariantQueryParams#CONSERVATION}
     * {@link VariantQueryParams#ALTERNATE_FREQUENCY}
     * {@link VariantQueryParams#REFERENCE_FREQUENCY}
     *
     * Stats filters:
     * {@link VariantQueryParams#STATS_MAF}
     * {@link VariantQueryParams#STATS_MGF}
     * {@link VariantQueryParams#MISSING_ALLELES}
     * {@link VariantQueryParams#MISSING_GENOTYPES}
     *
     * @param query Query to parse
     * @return List of sql filters
     */
    public List<String> getOtherFilters(Query query) {
        List<String> filters = new LinkedList<>();

        // Variant filters:
        if (isValidParam(query, TYPE)) {
            logger.warn("Unsupported filter " +  TYPE);
        }

        if (isValidParam(query, STUDIES)) {
            logger.warn("Unsupported filter " +  STUDIES);
        }

        if (isValidParam(query, FILES)) {
            logger.warn("Unsupported filter " +  FILES);
        }

        if (isValidParam(query, COHORTS)) {
            logger.warn("Unsupported filter " +  COHORTS);
        }

        if (isValidParam(query, GENOTYPE)) {
            logger.warn("Unsupported filter " +  GENOTYPE);
        }

        // Annotation filters:
        if (isValidParam(query, ANNOTATION_EXISTS)) {
            if (query.getBoolean(ANNOTATION_EXISTS.key())) {
                filters.add(Columns.FULL_ANNOTATION + " IS NOT NULL");
            } else {
                filters.add(Columns.FULL_ANNOTATION + " IS NULL");
            }
        }


        if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
            for (String so : query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key())) {
                //TODO: Catch number format exception
                int soInt = Integer.parseInt(so.toUpperCase().replace("SO:", ""));
                filters.add(soInt + " = ANY(" + Columns.SO + ")");
            }
        }

        if (isValidParam(query, ANNOT_XREF)) {
            logger.warn("Unsupported filter " +  ANNOT_XREF);
        }

        if (isValidParam(query, ANNOT_BIOTYPE)) {
            for (String biotype : query.getAsStringList(ANNOT_BIOTYPE.key())) {
                filters.add("'" + biotype + "' = ANY(" + Columns.BIOTYPE + ")");
            }
        }

        if (isValidParam(query, POLYPHEN)) {
            logger.warn("Unsupported filter " + POLYPHEN);
        }

        if (isValidParam(query, SIFT)) {
            logger.warn("Unsupported filter " + SIFT);
        }

        if (isValidParam(query, CONSERVATION)) {
            logger.warn("Unsupported filter " + CONSERVATION);
        }

        if (isValidParam(query, ALTERNATE_FREQUENCY)) {
            logger.warn("Unsupported filter " + ALTERNATE_FREQUENCY);
        }

        if (isValidParam(query, REFERENCE_FREQUENCY)) {
            logger.warn("Unsupported filter " + REFERENCE_FREQUENCY);
        }

        // Stats filters:


        if (isValidParam(query, STATS_MAF)) {
            logger.warn("Unsupported filter " +  STATS_MAF);
        }

        if (isValidParam(query, STATS_MGF)) {
            logger.warn("Unsupported filter " +  STATS_MGF);
        }

        if (isValidParam(query, MISSING_ALLELES)) {
            logger.warn("Unsupported filter " +  MISSING_ALLELES);
        }

        if (isValidParam(query, MISSING_GENOTYPES)) {
            logger.warn("Unsupported filter " +  MISSING_GENOTYPES);
        }

        return filters;
    }

    /**
     * Check if the object query contains the value param, is not null and, if is an string or a list, is not empty.
     *
     * isValidParam(new Query(), PARAM) == false
     * isValidParam(new Query(PARAM.key(), null), PARAM) == false
     * isValidParam(new Query(PARAM.key(), ""), PARAM) == false
     * isValidParam(new Query(PARAM.key(), Collections.emptyList()), PARAM) == false
     * isValidParam(new Query(PARAM.key(), 5), PARAM) == true
     * isValidParam(new Query(PARAM.key(), "sdfas"), PARAM) == true
     *
     * @param query Query to parse
     * @param param QueryParam to check
     * @return If is valid or not
     */
    public static boolean isValidParam(Query query, VariantQueryParams param) {
        Object value = query.getOrDefault(param.key(), null);
        return (value != null)
                && !(value instanceof String && ((String) value).isEmpty()
                || value instanceof Collection && ((Collection) value).isEmpty());
    }

}
