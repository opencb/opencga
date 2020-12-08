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

package org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.*;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.*;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsTask;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryOptions.COUNT;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper.Column;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.*;

/**
 * Created on 16/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSqlQueryParser {

    public static final List<String> DEFAULT_LOADED_GENOTYPES = Collections.unmodifiableList(Arrays.asList(
            ".", "./.",
            "0/0", "0|0",
            "0/1", "1/0", "1/1",
            "0/2", "1/2", "2/2",
            "0/3", "1/3", "2/3", "3/3",
            ".|.",
            "0|1", "1|0", "1|1",
            "0|2", "2|0", "2|1", "1|2", "2|2",
            "0|3", "1|3", "2|3", "3|3",
            "3|0", "3|1", "3|2"));
    private final GenomeHelper genomeHelper;
    private final String variantTable;
    private final Logger logger = LoggerFactory.getLogger(VariantSqlQueryParser.class);
    private final VariantStorageMetadataManager metadataManager;
    private final boolean clientSideSkip;

    private static final Map<String, String> SQL_OPERATOR;

    static {
        SQL_OPERATOR = new HashMap<>();
        SQL_OPERATOR.put("==", "=");
        SQL_OPERATOR.put("=~", "LIKE");
        SQL_OPERATOR.put("~", "LIKE");
        SQL_OPERATOR.put("!", "!=");
        SQL_OPERATOR.put(">>", ">");
        SQL_OPERATOR.put(">>=", ">=");
        SQL_OPERATOR.put("<<", "<");
        SQL_OPERATOR.put("<<=", "<=");
    }

    public VariantSqlQueryParser(GenomeHelper genomeHelper, String variantTable, VariantStorageMetadataManager metadataManager) {
        this(genomeHelper, variantTable, metadataManager, false);
    }

    public VariantSqlQueryParser(GenomeHelper genomeHelper, String variantTable,
                                 VariantStorageMetadataManager metadataManager, boolean clientSideSkip) {
        this.genomeHelper = genomeHelper;
        this.variantTable = variantTable;
        this.metadataManager = metadataManager;
        this.clientSideSkip = clientSideSkip;
    }

    @Deprecated
    public String parse(Query query, QueryOptions options) {
        ParsedVariantQuery variantQuery = new VariantQueryParser(null, metadataManager).parseQuery(query, options, true);
        return parse(variantQuery, options);
    }

    public String parse(ParsedVariantQuery variantQuery, QueryOptions options) {
        Query query = variantQuery.getQuery();

        StringBuilder sb = new StringBuilder("SELECT ");

        try {

            Set<Column> dynamicColumns = new HashSet<>();
            List<String> combinedFilters = new ArrayList<>();
            List<String> regionFilters = getRegionFilters(query, combinedFilters);
            List<String> filters = getOtherFilters(variantQuery, options, dynamicColumns);
            filters.addAll(combinedFilters);

            List<HintNode.Hint> hints = new ArrayList<>();
            if (DEFAULT_TABLE_TYPE != PTableType.VIEW && filters.isEmpty()) {
                // Only region filters. Hint no index usage
                hints.add(HintNode.Hint.NO_INDEX);
            }
            if (options.containsKey("HINT")) {
                for (String hint : options.getAsStringList("HINT")) {
                    hints.add(HintNode.Hint.valueOf(hint));
                }
            }
            if (!hints.isEmpty()) {
                sb.append("/*+ ").append(hints.stream().map(Object::toString).collect(Collectors.joining(","))).append(" */ ");
            }

            appendProjectedColumns(sb, variantQuery.getProjection(), options);
            appendFromStatement(sb, dynamicColumns);
            appendWhereStatement(sb, regionFilters, filters);
            appendOrderby(options, sb);
            appendLimitSkip(options, sb);

        } catch (VariantQueryException e) {
            e.setQuery(query);
            throw e;
        }
        return sb.toString();
    }

    private void appendOrderby(QueryOptions options, StringBuilder sb) {
        if (options.getBoolean(QueryOptions.COUNT)) {
            return;
        }
        if (options.getBoolean(QueryOptions.SORT)) {
            sb.append(" ORDER BY ").append(VariantColumn.CHROMOSOME.column()).append(',').append(VariantColumn.POSITION.column());

            String order = options.getString(QueryOptions.ORDER, QueryOptions.ASCENDING);
            if (order.equalsIgnoreCase(QueryOptions.ASCENDING) || order.equalsIgnoreCase("ASC")) {
                sb.append(" ASC ");
            } else {
                sb.append(" DESC ");
            }
        }
    }

    private void appendLimitSkip(QueryOptions options, StringBuilder sb) {
        if (options.getBoolean(QueryOptions.COUNT)) {
            return;
        }
        if (clientSideSkip) {
            int skip = Math.max(0, options.getInt(QueryOptions.SKIP));
            if (options.getInt(QueryOptions.LIMIT, -1) >= 0) {
                sb.append(" LIMIT ").append(skip + options.getInt(QueryOptions.LIMIT));
            }
        } else {
            if (options.getInt(QueryOptions.LIMIT, -1) >= 0) {
                sb.append(" LIMIT ").append(options.getInt(QueryOptions.LIMIT));
            }

            if (options.getInt(QueryOptions.SKIP, -1) >= 0) {
                sb.append(" OFFSET ").append(options.getInt(QueryOptions.SKIP));
            }
        }
    }

    /**
     * Select only the required columns.
     * <p>
     * Uses the params:
     * {@link VariantQueryParam#INCLUDE_STUDY}
     * {@link VariantQueryParam#INCLUDE_SAMPLE}
     * {@link VariantQueryParam#INCLUDE_FILE}
     * {@link VariantQueryParam#UNKNOWN_GENOTYPE}
     *
     * @param sb              SQLStringBuilder
     * @param projection      Query projection
     * @param options         other options
     * @return String builder
     */
    protected StringBuilder appendProjectedColumns(StringBuilder sb, VariantQueryProjection projection, QueryOptions options) {
        if (options.getBoolean(COUNT)) {
            return sb.append(" COUNT(*) ");
        } else {
            Set<VariantField> returnedFields = projection.getFields();
            Collection<Integer> studyIds = projection.getStudyIds();

            sb.append(VariantColumn.CHROMOSOME).append(',')
                    .append(VariantColumn.POSITION).append(',')
                    .append(VariantColumn.REFERENCE).append(',')
                    .append(VariantColumn.ALTERNATE).append(',')
                    .append(VariantColumn.TYPE);

            for (VariantQueryProjection.StudyVariantQueryProjection study : projection.getStudies().values()) {
                int studyId = study.getId();

                if (returnedFields.contains(VariantField.STUDIES)) {
                    Column studyColumn = VariantPhoenixHelper.getStudyColumn(studyId);
                    sb.append(",\"").append(studyColumn.column()).append('"');
                    sb.append(",\"").append(VariantPhoenixHelper.getFillMissingColumn(studyId).column()).append('"');
                    if (returnedFields.contains(VariantField.STUDIES_STATS)) {
                        for (Integer cohortId : study.getCohorts()) {
                            Column statsColumn = getStatsColumn(studyId, cohortId);
                            sb.append(",\"").append(statsColumn.column()).append('"');
                        }
                    }

                }

                if (returnedFields.contains(VariantField.STUDIES_FILES)) {
                    for (Integer fileId : study.getFiles()) {
                        sb.append(",\"");
                        buildFileColumnKey(studyId, fileId, sb);
                        sb.append('"');
                    }
                }
                if (returnedFields.contains(VariantField.STUDIES_SAMPLES)) {
                    for (Map.Entry<Integer, List<Integer>> entry : study.getMultiFileSamples().entrySet()) {
                        Integer sampleId = entry.getKey();
                        List<Integer> fileIds = entry.getValue();
                        sb.append(",\"");
                        buildSampleColumnKey(studyId, sampleId, sb);
                        sb.append('"');
                        if (!fileIds.isEmpty()) {
                            List<Integer> allFilesFromSampleId = metadataManager.getFileIdsFromSampleId(studyId, sampleId);
                            for (Integer fileId : fileIds) {
                                if (fileId.equals(allFilesFromSampleId.get(0))) {
                                    // Skip the first one
                                    continue;
                                }
                                sb.append(",\"");
                                buildSampleColumnKey(studyId, sampleId, fileId, sb);
                                sb.append('"');
                            }
                        }
                    }

                    // Check if any of the files from the included samples is not being returned.
                    // If don't, add it to the return list.
                    Set<Integer> fileIds = metadataManager.getFileIdsFromSampleIds(studyId, study.getSamples());
                    List<Integer> includeFiles = projection.getStudy(studyId).getFiles();
                    for (Integer fileId : fileIds) {
                        if (!includeFiles.contains(fileId)) {
                            sb.append(",\"");
                            buildFileColumnKey(studyId, fileId, sb);
                            sb.append('"');
                        }
                    }
                }
                if (returnedFields.contains(VariantField.STUDIES_SCORES)) {
                    for (VariantScoreMetadata variantScore : study.getStudyMetadata().getVariantScores()) {
                        sb.append(",\"");
                        sb.append(VariantPhoenixHelper.getVariantScoreColumn(variantScore.getStudyId(), variantScore.getId()));
                        sb.append('"');
                    }
                }
            }

            if (returnedFields.contains(VariantField.ANNOTATION)) {
                sb.append(',').append(VariantColumn.FULL_ANNOTATION);
                sb.append(',').append(VariantColumn.ANNOTATION_ID);

                int release = metadataManager.getProjectMetadata().getRelease();
                for (int i = 1; i <= release; i++) {
                    sb.append(',');
                    VariantPhoenixHelper.buildReleaseColumnKey(i, sb);
                }
            }

            return sb;
        }
    }

    protected void appendFromStatement(StringBuilder sb, Set<Column> dynamicColumns) {
        sb.append(" FROM ").append(getEscapedFullTableName(variantTable, genomeHelper.getConf()));

        if (!dynamicColumns.isEmpty()) {
            sb.append(dynamicColumns.stream()
                    .map(column -> SchemaUtil.getEscapedFullColumnName(column.column()) + " " + column.sqlType())
                    .collect(Collectors.joining(",", " ( ", " ) "))
            );
        }
    }

    protected StringBuilder appendWhereStatement(StringBuilder sb, List<String> regionFilters, List<String> filters) {
        if (!regionFilters.isEmpty() || !filters.isEmpty()) {
            sb.append(" WHERE");
        }

        appendFilters(sb, regionFilters, QueryOperation.OR);

        if (!filters.isEmpty() && !regionFilters.isEmpty()) {
            sb.append(" AND");
        }

        appendFilters(sb, filters, QueryOperation.AND);

        return sb;
    }

    protected String appendFilters(List<String> filters, QueryOperation logicalOperation) {
        return appendFilters(new StringBuilder(), filters, logicalOperation).toString();
    }

    protected StringBuilder appendFilters(StringBuilder sb, List<String> filters, QueryOperation logicalOperation) {
        String delimiter;
        if (logicalOperation == null) {
            if (filters.size() == 1) {
                return sb.append(" ( ").append(filters.get(0)).append(" ) ");
            } else {
                throw new VariantQueryException("Missing logical operation!");
            }
        }
        switch (logicalOperation) {
            case AND:
                delimiter = " ) AND ( ";
                break;
            case OR:
                delimiter = " ) OR ( ";
                break;
            default:
                throw new IllegalArgumentException("Unknown QueryOperation " + logicalOperation);
        }
        if (!filters.isEmpty()) {
            sb.append(filters.stream().collect(Collectors.joining(delimiter, " ( ( ", " ) )")));
        }
        return sb;
    }

    /**
     * Transform QueryParams that are inclusive.
     *
     * A variant will pass this filters if matches with ANY of this filters.
     *
     * {@link VariantQueryParam#REGION}
     *
     * Using annotation:
     * {@link VariantQueryParam#ID}
     * {@link VariantQueryParam#GENE}
     * {@link VariantQueryParam#ANNOT_XREF}
     * {@link VariantQueryParam#ANNOT_CLINVAR}
     * {@link VariantQueryParam#ANNOT_COSMIC}
     *
     * @param query Query to parse
     * @return List of region filters
     */
    protected List<String> getRegionFilters(Query query, List<String> otherFilters) {
        List<String> regionFilters = new LinkedList<>();

        if (isValidParam(query, REGION)) {
            List<Region> regions = Region.parseRegions(query.getString(REGION.key()), true);
            for (Region region : regions) {
                regionFilters.add(getRegionFilter(region));
            }
        }

        ParsedVariantQuery.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);

        // TODO: This should filter by ID from the VCF
        for (String id : variantQueryXref.getIds()) {
            regionFilters.add(buildFilter(VariantColumn.XREFS, "=", id));
        }
        for (String xrefs : variantQueryXref.getOtherXrefs()) {
            regionFilters.add(buildFilter(VariantColumn.XREFS, "=", xrefs));
        }
        if (!variantQueryXref.getVariants().isEmpty()) {
            regionFilters.add(getVariantFilter(variantQueryXref.getVariants()));
        }

        BiotypeConsquenceTypeFlagCombination combination = BiotypeConsquenceTypeFlagCombination.fromQuery(query);
        boolean flagCombined = combination.isFlag(); // Is flag being used in the combination?

        boolean onlyGeneRegionFilter = regionFilters.isEmpty();
        if (!variantQueryXref.getGenes().isEmpty()) {
            List<String> genes = variantQueryXref.getGenes();
            List<String> geneRegionFilters = new ArrayList<>();
            boolean skipGeneRegions = false;
            if (isValidParam(query, ANNOT_GENE_REGIONS)) {
                String geneRegions = query.getString(ANNOT_GENE_REGIONS.key());
                if (geneRegions.equals(SKIP_GENE_REGIONS)) {
                    skipGeneRegions = true;
                } else {
                    for (Region region : Region.parseRegions(geneRegions, true)) {
                        geneRegionFilters.add(getRegionFilter(region));
                    }
                }
            } else {
                throw new VariantQueryException("Error building query by genes '" + genes
                        + "', missing gene regions");
            }

            final List<String> combinedFilters;
            switch (combination) {
                case CT:
                    combinedFilters = combineGeneSo(query, genes);
                    break;
                case CT_FLAG:
                    combinedFilters = combineGeneSoFlag(query, genes);
                    break;
                case BIOTYPE_FLAG:
                    // FLAG can not be combined with GENE and BIOTYPE.
                    // Use only GeneBiotype
                    flagCombined = false;
                case BIOTYPE:
                    combinedFilters = combineGeneBiotype(query, genes);
                    break;
                case BIOTYPE_CT:
                    combinedFilters = combineGeneBiotypeSo(query, genes);
                    break;
                case BIOTYPE_CT_FLAG:
                    // Combine geneBiotypeSo and geneSoFlag
                    List<String> geneBiotypeSo = combineGeneBiotypeSo(query, genes);
                    List<String> geneSoFlag = combineGeneSoFlag(query, genes);
                    combinedFilters = Arrays.asList(
                            appendFilters(geneBiotypeSo, QueryOperation.OR),
                            appendFilters(geneSoFlag, QueryOperation.OR));
                    break;
                case FLAG:
                    // FLAG can not be combined with GENE alone. Skip combine!
                    flagCombined = false;
                case NONE:
                    combinedFilters = null;
                    break;
                default:
                    // This should never happen!
                    throw new IllegalStateException("Unsupported combination = " + combination);
            }

            if (combinedFilters == null) {
                regionFilters.addAll(geneRegionFilters);
            } else if (skipGeneRegions) {
                // Add combinedFilters as normal filters
                otherFilters.addAll(combinedFilters);
            } else {
                regionFilters.add(appendFilters(Arrays.asList(
                        appendFilters(geneRegionFilters, QueryOperation.OR),
                        appendFilters(combinedFilters, QueryOperation.OR)), QueryOperation.AND));
            }

            if (onlyGeneRegionFilter) {
                // If there are only gene region filter, remove ConsequenceType and Biotype filter
                query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                query.remove(ANNOT_BIOTYPE.key());
                if (flagCombined) {
                    query.remove(ANNOT_TRANSCRIPT_FLAG.key());
                }
            }
        }

//        if (regionFilters.isEmpty()) {
//            // chromosome != _METADATA
//            regionFilters.add(VariantColumn.CHROMOSOME + " != '" + genomeHelper.getMetaRowKeyString() + "'");
//        }
        return regionFilters;
    }

    private List<String> combineGeneSo(Query query, List<String> genes) {
        List<String> soList = query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key());
        Set<String> gnSoSet = new HashSet<>(genes.size() * soList.size());
        for (String gene : genes) {
            for (String so : soList) {
                int soNumber = parseConsequenceType(so);
                gnSoSet.add(VariantAnnotationToPhoenixConverter.combine(gene, soNumber));
            }
        }

        List<String> gnSoFilters = new ArrayList<>(gnSoSet.size());
        for (String gnSo : gnSoSet) {
            gnSoFilters.add(buildFilter(VariantColumn.GENE_SO, "=", gnSo));
        }
        return gnSoFilters;
    }

    private List<String> combineGeneBiotype(Query query, List<String> genes) {
        List<String> biotypes = query.getAsStringList(ANNOT_BIOTYPE.key());
        Set<String> gnBiotypeSet = new HashSet<>(genes.size() * biotypes.size());
        for (String gene : genes) {
            for (String biotype : biotypes) {
                gnBiotypeSet.add(VariantAnnotationToPhoenixConverter.combine(gene, biotype));
            }
        }
        List<String> gnBiotypeFilters = new ArrayList<>(gnBiotypeSet.size());
        for (String gnBiotype : gnBiotypeSet) {
            gnBiotypeFilters.add(buildFilter(VariantColumn.GENE_BIOTYPE, "=", gnBiotype));
        }
        return gnBiotypeFilters;
    }

    private List<String> combineGeneBiotypeSo(Query query, List<String> genes) {
        List<String> biotypes = query.getAsStringList(ANNOT_BIOTYPE.key());
        List<String> soList = query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key());
        Set<String> gnBiotypeSoSet = new HashSet<>(genes.size() * biotypes.size() * soList.size());
        for (String gene : genes) {
            for (String so : soList) {
                int soNumber = parseConsequenceType(so);
                for (String biotype : biotypes) {
                    gnBiotypeSoSet.add(VariantAnnotationToPhoenixConverter.combine(gene, biotype, soNumber));
                }
            }
        }
        List<String> gnBiotypeSoFilters = new ArrayList<>(gnBiotypeSoSet.size());
        for (String gnBiotypeSo : gnBiotypeSoSet) {
            gnBiotypeSoFilters.add(buildFilter(VariantColumn.GENE_BIOTYPE_SO, "=", gnBiotypeSo));
        }
        return gnBiotypeSoFilters;
    }

    private List<String> combineGeneSoFlag(Query query, List<String> genes) {
        List<String> flags = query.getAsStringList(ANNOT_TRANSCRIPT_FLAG.key());
        List<String> soList = query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key());
        Set<String> gnBiotypeSoSet = new HashSet<>(genes.size() * flags.size() * soList.size());
        for (String gene : genes) {
            for (String so : soList) {
                int soNumber = parseConsequenceType(so);
                for (String flag : flags) {
                    gnBiotypeSoSet.add(VariantAnnotationToPhoenixConverter.combine(gene, soNumber, flag));
                }
            }
        }
        List<String> gnBiotypeSoFilters = new ArrayList<>(gnBiotypeSoSet.size());
        for (String gnBiotypeSo : gnBiotypeSoSet) {
            gnBiotypeSoFilters.add(buildFilter(VariantColumn.GENE_BIOTYPE_SO, "=", gnBiotypeSo));
        }
        return gnBiotypeSoFilters;
    }

    private String getVariantFilter(List<Variant> variants) {
        StringBuilder sb = new StringBuilder().append("(")
                .append(VariantColumn.CHROMOSOME).append(", ")
                .append(VariantColumn.POSITION).append(", ")
                .append(VariantColumn.REFERENCE).append(", ")
                .append(VariantColumn.ALTERNATE).append(") IN (");
        Iterator<Variant> iterator = variants.iterator();
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            sb.append("('").append(checkStringValue(variant.getChromosome())).append("', ")
                    .append(variant.getStart()).append(", ")
                    .append('\'').append(checkStringValue(variant.getReference())).append("', ")
                    .append('\'').append(checkStringValue(variant.getAlternate())).append("') ");
            if (iterator.hasNext()) {
                sb.append(',');
            }
        }
        sb.append(')');
        return sb.toString();
    }

//    private String getRegionFilter(Region region) {
//        if (region.getStart() == region.getEnd()) {
//            return String.format("(%s,%s) = ('%s',%s)",
//                    VariantColumn.CHROMOSOME,
//                    VariantColumn.POSITION,
//                    region.getChromosome(), region.getStart());
//        } else {
//            return String.format("(%s,%s) BETWEEN ('%s',%s) AND ('%s',%s)",
//                    VariantColumn.CHROMOSOME,
//                    VariantColumn.POSITION,
//                    region.getChromosome(), region.getStart(),
//                    region.getChromosome(), region.getEnd());
//        }
//    }

    private String getRegionFilter(Region region) {
        List<String> subFilters = new ArrayList<>(3);
        subFilters.add(buildFilter(VariantColumn.CHROMOSOME, "=", region.getChromosome()));
        if (region.getStart() > 1) {
            subFilters.add(buildFilter(VariantColumn.POSITION, ">=", region.getStart()));
        }
        if (region.getEnd() < Integer.MAX_VALUE) {
            subFilters.add(buildFilter(VariantColumn.POSITION, "<=", region.getEnd()));
        }
        return appendFilters(subFilters, QueryOperation.AND);
    }

    /**
     * Transform QueryParams that are exclusive.
     * <p>
     * A variant will pass this filters if matches with ALL of this filters.
     * <p>
     * Variant filters:
     * {@link VariantQueryParam#REFERENCE}
     * {@link VariantQueryParam#ALTERNATE}
     * {@link VariantQueryParam#TYPE}
     * {@link VariantQueryParam#STUDY}
     * {@link VariantQueryParam#FILE}
     * {@link VariantQueryParam#SAMPLE_DATA}
     * {@link VariantQueryParam#FILE_DATA}
     * {@link VariantQueryParam#FILTER}
     * {@link VariantQueryParam#QUAL}
     * {@link VariantQueryParam#COHORT}
     * {@link VariantQueryParam#GENOTYPE}
     * {@link VariantQueryParam#RELEASE}
     * <p>
     * Annotation filters:
     * {@link VariantQueryParam#ANNOTATION_EXISTS}
     * {@link VariantQueryParam#ANNOT_CONSEQUENCE_TYPE}
     * {@link VariantQueryParam#ANNOT_BIOTYPE}
     * {@link VariantQueryParam#ANNOT_POLYPHEN}
     * {@link VariantQueryParam#ANNOT_SIFT}
     * {@link VariantQueryParam#ANNOT_CONSERVATION}
     * {@link VariantQueryParam#ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY}
     * {@link VariantQueryParam#ANNOT_POPULATION_ALTERNATE_FREQUENCY}
     * {@link VariantQueryParam#ANNOT_POPULATION_REFERENCE_FREQUENCY}
     * <p>
     * {@link VariantQueryParam#ANNOT_TRANSCRIPT_FLAG}
     * {@link VariantQueryParam#ANNOT_GENE_TRAIT_ID}
     * {@link VariantQueryParam#ANNOT_GENE_TRAIT_NAME}
     * {@link VariantQueryParam#ANNOT_HPO}
     * {@link VariantQueryParam#ANNOT_GO}
     * {@link VariantQueryParam#ANNOT_EXPRESSION}
     * {@link VariantQueryParam#ANNOT_PROTEIN_KEYWORD}
     * {@link VariantQueryParam#ANNOT_DRUG}
     * {@link VariantQueryParam#ANNOT_FUNCTIONAL_SCORE}
     * {@link VariantQueryParam#ANNOT_CLINICAL_SIGNIFICANCE}
     * <p>
     * Stats filters:
     * {@link VariantQueryParam#STATS_ALT}
     * {@link VariantQueryParam#STATS_REF}
     * {@link VariantQueryParam#STATS_MAF}
     * {@link VariantQueryParam#STATS_MGF}
     * {@link VariantQueryParam#MISSING_ALLELES}
     * {@link VariantQueryParam#MISSING_GENOTYPES}
     *
     * @param variantQuery   Query to parse
     * @param options        Options
     * @param dynamicColumns Initialized empty set to be filled with dynamic columns required by the queries
     * @return List of sql filters
     */
    protected List<String> getOtherFilters(ParsedVariantQuery variantQuery, QueryOptions options, final Set<Column> dynamicColumns) {
        List<String> filters = new LinkedList<>();

        // Variant filters:
        addVariantFilters(variantQuery, options, filters);

        // Annotation filters:
        addAnnotFilters(variantQuery, dynamicColumns, filters);

        // Stats filters:
        addStatsFilters(variantQuery, filters);

        return filters;
    }

    protected void addVariantFilters(ParsedVariantQuery variantQuery, QueryOptions options, List<String> filters) {
        Query query = variantQuery.getQuery();
        addQueryFilter(query, REFERENCE, VariantColumn.REFERENCE, filters);

        addQueryFilter(query, ALTERNATE, VariantColumn.ALTERNATE, filters);

        addQueryFilter(query, TYPE, VariantColumn.TYPE, filters);

        final StudyMetadata defaultStudyMetadata = variantQuery.getStudyQuery().getDefaultStudy();
        if (isValidParam(query, STUDY)) {
            String value = query.getString(STUDY.key());
            QueryOperation operation = checkOperator(value);
            List<String> values = splitValue(value, operation);
            StringBuilder sb = new StringBuilder();
            Iterator<String> iterator = values.iterator();
            Map<String, Integer> studies = metadataManager.getStudies(options);
            Set<Integer> notNullStudies = new HashSet<>();
            while (iterator.hasNext()) {
                String study = iterator.next();
                Integer studyId = metadataManager.getStudyId(study, false, studies);
                if (isNegated(study)) {
                    sb.append("\"").append(getStudyColumn(studyId).column()).append("\" IS NULL ");
                } else {
                    notNullStudies.add(studyId);
                    sb.append("\"").append(getStudyColumn(studyId).column()).append("\" IS NOT NULL ");
                }
                if (iterator.hasNext()) {
                    if (operation == null || operation.equals(QueryOperation.AND)) {
                        sb.append(" AND ");
                    } else {
                        sb.append(" OR ");
                    }
                }
            }
            // Skip this filter if contains all the existing studies (union of all studies), or if there is only one study
            if (studies.size() == notNullStudies.size() && notNullStudies.containsAll(studies.values())
                    && (operation == QueryOperation.OR || studies.size() == 1)) {
                logger.debug("Skip studies filter to phoenix");
            } else {
                filters.add(sb.toString());
            }
        }
//        else {
//            StringBuilder sb = new StringBuilder();
//            for (Iterator<Integer> iterator = studyIds.iterator(); iterator.hasNext();) {
//                Integer studyId = iterator.next();
//                sb.append('"').append(getStudyColumn(studyId).column()).append("\" IS NOT NULL");
//                if (iterator.hasNext()) {
//                    sb.append(" OR ");
//                }
//            }
//            filters.add(sb.toString());
//        }
        List<String> includeFiles = VariantQueryProjectionParser.getIncludeFilesList(query);
        QueryOperation filtersOperation = null;
        List<String> filterValues = Collections.emptyList();
        if (isValidParam(query, FILTER)) {
            String value = query.getString(FILTER.key());
            filtersOperation = checkOperator(value);
            filterValues = splitValue(value, filtersOperation);
            if (!filterValues.isEmpty()) {
                if (CollectionUtils.isEmpty(includeFiles)) {
                    throw VariantQueryException.malformedParam(FILTER, value, "Missing \"" + FILE.key() + "\" filter");
                }
            }
        }

        QueryOperation qualOperation = null;
        List<String> qualValues = Collections.emptyList();
        if (isValidParam(query, QUAL)) {
            String value = query.getString(QUAL.key());
            qualOperation = checkOperator(value);
            qualValues = splitValue(value, qualOperation);
            if (!qualValues.isEmpty()) {
                if (CollectionUtils.isEmpty(includeFiles)) {
                    throw VariantQueryException.malformedParam(QUAL, value, "Missing \"" + FILE.key() + "\" filter");
                }
            }
        }

        if (isValidParam(query, FILE_DATA)) {
            addFileDataFilter(query, filters, defaultStudyMetadata);
        }

        if (isValidParam(query, SAMPLE_DATA)) {
            addFormatFilter(query, filters, defaultStudyMetadata);
        }

        List<String> files = Collections.emptyList();
        List<Pair<Integer, Integer>> fileIds = Collections.emptyList();
        QueryOperation fileOperation = null;
        if (isValidParam(query, FILE)) {
            String value = query.getString(FILE.key());
            fileOperation = checkOperator(value);
            files = splitValue(value, fileOperation);
        } else {
            if (!qualValues.isEmpty() || !filterValues.isEmpty()) {
                files = includeFiles;
                fileOperation = QueryOperation.OR;
            }
        }

        if (!files.isEmpty()) {
            fileIds = new ArrayList<>(files.size());
            StringBuilder sb = new StringBuilder();
            for (Iterator<String> iterator = files.iterator(); iterator.hasNext();) {
                String file = iterator.next();
                Pair<Integer, Integer> fileIdPair = metadataManager.getFileIdPair(file, false, defaultStudyMetadata);
                fileIds.add(fileIdPair);

                sb.append(" ( ");
                if (isNegated(file)) {
                    // ( "FILE" IS NULL OR "FILE"[3] != 'N' )

                    sb.append('"');
                    buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                    sb.append("\" IS NULL ");

                    sb.append(" OR ");

                    sb.append('"');
                    buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                    sb.append('"');
                    // Arrays in SQL are 1-based.
                    sb.append('[').append(HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX + 1).append(']');
                    sb.append(" != '").append(VariantOverlappingStatus.NONE.toString()).append('\'');

                } else {
                    // ( "FILE"[3] = 'N' )
                    sb.append('"');
                    buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                    sb.append('"');
                    // Arrays in SQL are 1-based.
                    sb.append('[').append(HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX + 1).append(']');
                    sb.append(" = '").append(VariantOverlappingStatus.NONE.toString()).append('\'');

                    if (!filterValues.isEmpty()) {
                        // ( "FILE"[3] = 'N' AND ( "FILE"[5] = 'FILTER_1' OR "FILE"[5] = 'FILTER_2' ) )
                        sb.append(" AND ");
                        addFileFilterFieldFilter(filtersOperation, filterValues, sb, fileIdPair);
                    }
                    if (!qualValues.isEmpty()) {
                        // ( "FILE"[3] = 'N' AND ( "FILE"[5] = 'FILTER_1' OR "FILE"[5] = 'FILTER_2' ) )
                        sb.append(" AND ( ");
                        for (int i = 0; i < qualValues.size(); i++) {
                            String qualValue = qualValues.get(i);
                            String[] strings = splitOperator(qualValue);
                            String op = strings[1];
                            String qual = strings[2];

                            if (i > 0 && qualOperation != null) {
                                sb.append(' ').append(qualOperation.name()).append(' ');
                            }

                            sb.append("TO_NUMBER(");
                            sb.append('"');
                            buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                            sb.append('"');

                            // Arrays in SQL are 1-based.
                            sb.append('[').append(HBaseToStudyEntryConverter.FILE_QUAL_IDX + 1).append(']');
                            sb.append(')');

                            double parsedValue = parseDouble(qual, QUAL, qualValue);
                            sb.append(parseNumericOperator(op)).append(' ').append(parsedValue);

                            if (op.startsWith(">>") || op.startsWith("<<")) {
                                sb.append(" OR \"");
                                buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                                sb.append('"');

                                // Arrays in SQL are 1-based.
                                sb.append('[').append(HBaseToStudyEntryConverter.FILE_QUAL_IDX + 1).append("] IS NULL");
                            }
                        }
                        sb.append(" ) ");
                    }
                }
                sb.append(" ) ");
                if (iterator.hasNext()) {
                    if (fileOperation == null) {
                        // This should never happen!
                        throw new VariantQueryException("Unexpected error");
                    } else if (fileOperation.equals(QueryOperation.AND)) {
                        sb.append(" AND ");
                    } else {
                        sb.append(" OR ");
                    }
                }
            }
            filters.add(sb.toString());
        }

        if (isValidParam(query, COHORT)) {
            for (String cohort : query.getAsStringList(COHORT.key())) {
                boolean negated = false;
                if (isNegated(cohort)) {
                    cohort = removeNegation(cohort);
                    negated = true;
                }
                String[] studyCohort = splitStudyResource(cohort);
                StudyMetadata studyMetadata;
                if (studyCohort.length == 2) {
                    studyMetadata = metadataManager.getStudyMetadata(studyCohort[0]);
                    cohort = studyCohort[1];
                } else if (studyCohort.length == 1) {
                    studyMetadata = defaultStudyMetadata;
                } else {
                    throw VariantQueryException.malformedParam(COHORT, query.getString((COHORT.key())), "Expected {study}:{cohort}");
                }
                Integer cohortId = metadataManager.getCohortId(studyMetadata.getId(), cohort);
                if (cohortId == null) {
                    throw VariantQueryException.cohortNotFound(cohort, studyMetadata.getId(), metadataManager);
                }
                Column column = getStatsColumn(studyMetadata.getId(), cohortId);
                if (negated) {
                    filters.add("\"" + column + "\" IS NULL");
                } else {
                    filters.add("\"" + column + "\" IS NOT NULL");
                }
            }
        }

        ParsedQuery<KeyOpValue<SampleMetadata, List<String>>> genotypesQuery = variantQuery.getStudyQuery().getGenotypes();
        if (genotypesQuery != null) {
            // Remove (if any) the GenotypeClass#NA_GT_VALUE from the genotypes map.
            Iterator<KeyOpValue<SampleMetadata, List<String>>> iterator = genotypesQuery.getValues().iterator();
            while (iterator.hasNext()) {
                KeyOpValue<SampleMetadata, List<String>> keyOpValue = iterator.next();
                List<String> gts = keyOpValue.getValue();
                if (gts.contains(GenotypeClass.NA_GT_VALUE)) {
                    if (gts.size() == 1) {
                        iterator.remove();
                    } else {
                        gts = new LinkedList<>(gts);
                        gts.remove(GenotypeClass.NA_GT_VALUE);
                        keyOpValue.setValue(gts);
                    }
                }
            }

            List<String> gtFilters = new ArrayList<>(genotypesQuery.getValues().size());
            for (KeyOpValue<SampleMetadata, List<String>> keyOpValue : genotypesQuery.getValues()) {

                SampleMetadata sampleMetadata = keyOpValue.getKey();
                int studyId = sampleMetadata.getStudyId();
                int sampleId = sampleMetadata.getId();
                List<String> genotypes = keyOpValue.getValue();

                List<String> sampleGtFilters = new ArrayList<>(genotypes.size());
                final boolean negated;
                if (genotypes.stream().allMatch(VariantQueryUtils::isNegated)) {
                    negated = true;
                } else if (genotypes.stream().anyMatch(VariantQueryUtils::isNegated)) {
                    throw VariantQueryException.malformedParam(GENOTYPE, query.getString(GENOTYPE.key()),
                            "Can not mix negated and not negated genotypes");
                } else {
                    negated = false;
                }
                for (String genotype : genotypes) {
                    if (negated) {
                        genotype = removeNegation(genotype);
                    }
                    List<Integer> sampleFiles = new ArrayList<>();
                    if (VariantStorageEngine.SplitData.MULTI.equals(sampleMetadata.getSplitData())) {
                        if (fileIds.isEmpty()) {
                            sampleFiles.add(null); // First file does not have the fileID in the column name
                            List<Integer> fileIdsFromSampleId = sampleMetadata.getFiles();
                            sampleFiles.addAll(fileIdsFromSampleId.subList(1, fileIdsFromSampleId.size()));
                        } else {
                            for (Pair<Integer, Integer> fileIdPair : fileIds) {
                                if (fileIdPair.getKey().equals(studyId)) {
                                    Integer fileId = fileIdPair.getValue();
                                    int idx = sampleMetadata.getFiles().indexOf(fileId);
                                    if (idx == 0) {
                                        sampleFiles.add(null); // First file does not have the fileID in the column name
                                    } else if (idx > 0) {
                                        sampleFiles.add(fileId); // First file does not have the fileID in the column name
                                    }
                                }
                            }
                        }
                    } else {
                        sampleFiles.add(null); // First file does not have the fileID in the column name
                    }
                    for (Integer sampleFile : sampleFiles) {
                        String key;
                        if (sampleFile == null) {
                            key = buildSampleColumnKey(studyId, sampleId, new StringBuilder()).toString();
                        } else {
                            key = buildSampleColumnKey(studyId, sampleId, sampleFile, new StringBuilder()).toString();
                        }
                        final String filter;
                        if (FillGapsTask.isHomRefDiploid(genotype)) {
                            if (negated) {
                                filter = '"' + key + "\" IS NOT NULL AND \"" + key + "\"[1] != '" + genotype + '\'';
                            } else {
                                filter = "( \"" + key + "\"[1] = '" + genotype + "' OR \"" + key + "\" IS NULL )";
                            }
                        } else {
                            if (negated) {
                                filter = "( \"" + key + "\"[1] != '" + genotype + "' OR \"" + key + "\" IS NULL )";
                            } else {
                                filter = '"' + key + "\"[1] = '" + genotype + '\'';
                            }
                        }
                        sampleGtFilters.add(filter);
                    }
                }
                if (negated) {
                    gtFilters.add(appendFilters(sampleGtFilters, QueryOperation.AND));
                } else {
                    gtFilters.add(appendFilters(sampleGtFilters, QueryOperation.OR));
                }
            }
            filters.add(appendFilters(gtFilters, genotypesQuery.getOperation()));
        }

        if (isValidParam(query, SCORE)) {
            addQueryFilter(query, SCORE, (kov, v) -> {
                String key = kov[0];
                String[] studyResource = splitStudyResource(key);
                int studyId;
                int scoreId;
                if (studyResource.length == 1) {
                    studyId = defaultStudyMetadata.getId();
                    scoreId = metadataManager.getVariantScoreMetadata(defaultStudyMetadata, studyResource[0]).getId();
                } else {
                    studyId = metadataManager.getStudyId(studyResource[0]);
                    scoreId = metadataManager.getVariantScoreMetadata(studyId, studyResource[1]).getId();
                }
                return VariantPhoenixHelper.getVariantScoreColumn(studyId, scoreId);
            }, null, filters, null, 1);
        }

        if (isValidParam(query, RELEASE)) {
            int release = query.getInt(RELEASE.key(), -1);
            if (release <= 0) {
                throw VariantQueryException.malformedParam(RELEASE, query.getString(RELEASE.key()));
            }

            StringBuilder releaseFilters = new StringBuilder();
            releaseFilters.append(buildFilter(getReleaseColumn(1), "=", true));
            for (int i = 2; i <= release; i++) {
                releaseFilters.append(" OR ").append(buildFilter(getReleaseColumn(i), "=", true));
            }
            filters.add(releaseFilters.toString());
        }
    }

    private void addFileFilterFieldFilter(QueryOperation filtersOperation, List<String> filterValues, StringBuilder sb,
                                          Pair<Integer, Integer> fileIdPair) {
        sb.append(" ( ");
        for (int i = 0; i < filterValues.size(); i++) {
            String filter = checkStringValue(filterValues.get(i));
            boolean negated = isNegated(filter);
            if (negated) {
                filter = removeNegation(filter);
            }

            if (i > 0 && filtersOperation != null) {
                sb.append(' ').append(filtersOperation.name()).append(' ');
            }

            sb.append('"');
            buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
            sb.append('"');

            // Arrays in SQL are 1-based.
            sb.append('[').append(HBaseToStudyEntryConverter.FILE_FILTER_IDX + 1).append(']');
            if (filter.equals(VCFConstants.PASSES_FILTERS_v4) || filter.contains(VCFConstants.FILTER_CODE_SEPARATOR)) {
                if (negated) {
                    sb.append(" != '");
                } else {
                    sb.append(" = '");
                }
                sb.append(filter).append('\'');
            } else {
                if (negated) {
                    sb.append(" NOT ");
                }
                sb.append(" LIKE '%").append(filter).append("%'");
            }
        }
        sb.append(" ) ");
    }

    private void addFileDataFilter(Query query, List<String> filters, StudyMetadata defaultStudyMetadata) {
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> parsedQuery = parseFileData(query);
        QueryOperation fileDataOperation = parsedQuery.getOperation();
//        Map<String, String> infoValuesMap = pair.getValue();

        if (!parsedQuery.getValues().isEmpty()) {
            List<String> fixedAttributes = HBaseToVariantConverter.getFixedAttributes(defaultStudyMetadata);

            StringBuilder sb = new StringBuilder();
            boolean firstElement = true;
            for (KeyValues<String, KeyOpValue<String, String>> fileDataValues : parsedQuery.getValues()) {
                if (firstElement) {
                    firstElement = false;
                } else {
                    sb.append(fileDataOperation.toString());
                }

                sb.append(" ( ");

                Pair<Integer, Integer> fileIdPair = metadataManager
                        .getFileIdPair(fileDataValues.getKey(), false, defaultStudyMetadata);

                boolean firstSubElement = true;
                for (KeyOpValue<String, String> keyOpValue : fileDataValues.getValues()) {
                    if (firstSubElement) {
                        firstSubElement = false;
                    } else {
                        sb.append(fileDataValues.getOperation().toString());
                    }

                    sb.append(" ( ");

                    String fileDataKey = keyOpValue.getKey();
                    String op = keyOpValue.getOp();
                    String filterValue = keyOpValue.getValue();

                    if (fileDataKey.equals(StudyEntry.FILTER)) {
                        Values<String> value = splitValues(filterValue);
                        addFileFilterFieldFilter(value.getOperation(), value.getValues(), sb, fileIdPair);
                    } else {
                        boolean toNumber;
                        int arrayIdx;
                        if (fileDataKey.equals(StudyEntry.QUAL)) {
                            arrayIdx = HBaseToStudyEntryConverter.FILE_QUAL_IDX;
                            toNumber = true;
                        } else {
                            int infoIdx = fixedAttributes.indexOf(fileDataKey);
                            arrayIdx = HBaseToStudyEntryConverter.FILE_INFO_START_IDX + infoIdx;

                            VariantFileHeaderComplexLine infoLine = defaultStudyMetadata.getVariantHeaderLines("INFO").get(fileDataKey);
                            toNumber = infoLine.getType().equals("Float") || infoLine.getType().equals("Integer");
                        }
                        // Arrays in SQL are 1-based.
                        arrayIdx++;

                        if (toNumber) {
                            sb.append("TO_NUMBER(");
                        }
                        sb.append('"');
                        buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                        sb.append('"');

                        sb.append('[').append(arrayIdx).append(']');

                        if (toNumber) {
                            sb.append(')');
                            double parsedValue = parseDouble(filterValue, FILE_DATA, keyOpValue.toQuery());
                            sb.append(parseNumericOperator(op)).append(' ').append(parsedValue);

                            if (op.startsWith(">>") || op.startsWith("<<")) {
                                sb.append(" OR \"");
                                buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue(), sb);
                                sb.append('"');

                                sb.append('[').append(arrayIdx).append("] IS NULL");
                            }
                        } else {
                            checkStringValue(filterValue);
                            sb.append(parseOperator(op)).append(" '").append(filterValue).append('\'');
                        }
                    }
                    sb.append(" ) ");
                }
                sb.append(" ) ");
            }
            filters.add(sb.toString());
        }
    }

    private void addFormatFilter(Query query, List<String> filters, StudyMetadata defaultStudyMetadata) {
        Pair<QueryOperation, Map<String, String>> pair = VariantQueryUtils.parseSampleData(query);
        QueryOperation formatOperation = pair.getKey();
        Map<String, String> formatValuesMap = pair.getValue();

        if (!formatValuesMap.isEmpty()) {
            List<String> fixedFormat = HBaseToVariantConverter.getFixedFormat(defaultStudyMetadata);

            StringBuilder sb = new StringBuilder();
            int i = -1;
            for (Map.Entry<String, String> entry : formatValuesMap.entrySet()) {
                i++;

                String formatValues = entry.getValue();
                int sampleId = metadataManager.getSampleId(defaultStudyMetadata.getId(), entry.getKey());
                Pair<QueryOperation, List<String>> formatPair = splitValue(formatValues);
                for (String formatValue : formatPair.getValue()) {

                    if (sb.length() > 0) {
                        sb.append(formatOperation.toString());
                    }
                    sb.append(" ( ");

                    String[] strings = splitOperator(formatValue);
                    String format = strings[0];
                    String op = strings[1];
                    String filterValue = strings[2];

                    int formatIdx = fixedFormat.indexOf(format);

                    VariantFileHeaderComplexLine formatLine = defaultStudyMetadata.getVariantHeaderLines("FORMAT").get(format);

                    boolean toNumber = formatLine.getType().equals("Float") || formatLine.getType().equals("Integer");
                    if (toNumber) {
                        sb.append("TO_NUMBER(");
                    }
                    sb.append('"');
                    buildSampleColumnKey(defaultStudyMetadata.getId(), sampleId, sb);
                    sb.append('"');

                    // Arrays in SQL are 1-based.
                    sb.append('[').append(formatIdx + 1).append(']');

                    if (toNumber) {
                        sb.append(')');
                        double parsedValue = parseDouble(filterValue, FILE_DATA, formatValues);
                        sb.append(parseNumericOperator(op)).append(' ').append(parsedValue);

                        if (op.startsWith(">>") || op.startsWith("<<")) {
                            sb.append(" OR \"");
                            buildSampleColumnKey(defaultStudyMetadata.getId(), sampleId, sb);
                            sb.append('"');

                            // Arrays in SQL are 1-based.
                            sb.append('[').append(formatIdx + 1).append("] IS NULL");
                        }
                    } else {
                        checkStringValue(filterValue);
                        sb.append(parseOperator(op)).append(" '").append(filterValue).append('\'');
                    }

                    sb.append(" ) ");
                }
            }
            filters.add(sb.toString());
        }
    }

    private void unsupportedFilter(Query query, VariantQueryParam param) {
        if (isValidParam(query, param)) {
            String warn = "Unsupported filter \"" + param + "\"";
//            warnings.add(warn);
            logger.warn(warn);
        }
    }

    protected void addAnnotFilters(ParsedVariantQuery variantQuery, Set<Column> dynamicColumns, List<String> filters) {
        Query query = variantQuery.getQuery();
        if (isValidParam(query, ANNOTATION_EXISTS)) {
            if (query.getBoolean(ANNOTATION_EXISTS.key())) {
                filters.add(VariantColumn.FULL_ANNOTATION + " IS NOT NULL");
            } else {
                filters.add(VariantColumn.FULL_ANNOTATION + " IS NULL");
            }
        }

        BiotypeConsquenceTypeFlagCombination combination = BiotypeConsquenceTypeFlagCombination.fromQuery(query);
        switch (combination) {
            case CT:
                addQueryFilter(query, ANNOT_CONSEQUENCE_TYPE, VariantColumn.SO, filters, VariantQueryUtils::parseConsequenceType);
                break;
            case FLAG:
                addQueryFilter(query, ANNOT_TRANSCRIPT_FLAG, VariantColumn.TRANSCRIPT_FLAGS, filters);
                break;
            case BIOTYPE:
                addQueryFilter(query, ANNOT_BIOTYPE, VariantColumn.BIOTYPE, filters);
                break;

            case BIOTYPE_FLAG:
                // FLAG can not be combined with BIOTYPE.
                addQueryFilter(query, ANNOT_BIOTYPE, VariantColumn.BIOTYPE, filters);
                addQueryFilter(query, ANNOT_TRANSCRIPT_FLAG, VariantColumn.TRANSCRIPT_FLAGS, filters);
                break;
            case BIOTYPE_CT:
                addSoBiotypeCombination(query, filters);
                break;
            case CT_FLAG:
                addSoFlagCombination(query, filters);
                break;
            case BIOTYPE_CT_FLAG:
                addSoBiotypeCombination(query, filters);
                addSoFlagCombination(query, filters);
                break;
            case NONE:
                // Some values from the TranscriptFlag are not considered for the combinations
                addQueryFilter(query, ANNOT_TRANSCRIPT_FLAG, VariantColumn.TRANSCRIPT_FLAGS, filters);
                break;
            default:
                // This should never happen!
                throw new IllegalStateException("Unsupported combination = " + combination);
        }

        addQueryFilter(query, ANNOT_SIFT, (keyOpValue, rawValue) -> {
            if (StringUtils.isNotEmpty(keyOpValue[0])) {
                throw VariantQueryException.malformedParam(ANNOT_SIFT, Arrays.toString(keyOpValue));
            }
            if (NumberUtils.isParsable(keyOpValue[2])) {
                return VariantColumn.SIFT;
            } else {
                return VariantColumn.SIFT_DESC;
            }
        }, null, filters, null, null, op -> op.contains(">") ? 2 : op.contains("<") ? 1 : -1);

        addQueryFilter(query, ANNOT_POLYPHEN, (keyOpValue, rawValue) -> {
            if (StringUtils.isNotEmpty(keyOpValue[0])) {
                throw VariantQueryException.malformedParam(ANNOT_POLYPHEN, Arrays.toString(keyOpValue));
            }
            if (NumberUtils.isParsable(keyOpValue[2])) {
                return VariantColumn.POLYPHEN;
            } else {
                return VariantColumn.POLYPHEN_DESC;
            }
        }, null, filters, null, null, op -> op.contains(">") ? 2 : op.contains("<") ? 1 : -1);

        addQueryFilter(query, ANNOT_PROTEIN_SUBSTITUTION, (keyOpValue, rawValue) -> {
            if (keyOpValue[0].equalsIgnoreCase("sift")) {
                if (NumberUtils.isParsable(keyOpValue[2])) {
                    return VariantColumn.SIFT;
                } else {
                    return VariantColumn.SIFT_DESC;
                }
            } else if (keyOpValue[0].equalsIgnoreCase("polyphen")) {
                if (NumberUtils.isParsable(keyOpValue[2])) {
                    return VariantColumn.POLYPHEN;
                } else {
                    return VariantColumn.POLYPHEN_DESC;
                }
            } else {
                throw VariantQueryException.malformedParam(ANNOT_PROTEIN_SUBSTITUTION, Arrays.toString(keyOpValue));
            }
        }, null, filters, null, null, op -> op.contains(">") ? 2 : op.contains("<") ? 1 : -1);

        addQueryFilter(query, ANNOT_CONSERVATION,
                (keyOpValue, rawValue) -> getConservationScoreColumn(keyOpValue[0], rawValue, true), null, filters);

        /*
         * maf < 0.3 --> PF < 0.3 OR PF >= 0.7
         * maf > 0.3 --> PF > 0.3 AND PF <= 0.7
         */
        addQueryFilter(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY,
                (keyOpValue, s) -> {
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
//                    dynamicColumns.add(column);
                    return column;
                }, null,
                filters, keyOpValue -> {
                    String op = keyOpValue[1];
                    double value = Double.parseDouble(keyOpValue[2]);
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
                    if (op.startsWith("<")) {
                        String f = " OR \"" + column.column() + "\"[2] " + op + " " + value;
                        if (!DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS.contains(column)) {
                            // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                            f += " OR \"" + column.column() + "\"[2] IS NULL";
                        }
                        return f;
                    } else if (op.startsWith(">")) {
                        return " AND \"" + column.column() + "\"[2] " + op + " " + value;
                    } else {
                        throw VariantQueryException.malformedParam(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, Arrays.toString(keyOpValue),
                                "Unable to use operator " + op + " with this query.");
                    }
                }, 1);

        addQueryFilter(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY,
                (keyOpValue, s) -> {
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
//                    dynamicColumns.add(column);
                    return column;
                }, null,
                filters, keyOpValue -> {
                    // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
                    if (keyOpValue[1].startsWith("<") && !DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS.contains(column)) {
                        return " OR \"" + column.column() + "\"[2] IS NULL";
                    }
                    return "";
                }, 2);

        addQueryFilter(query, ANNOT_POPULATION_REFERENCE_FREQUENCY,
                (keyOpValue, s) -> {
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
//                    dynamicColumns.add(column);
                    return column;
                }, null,
                filters, keyOpValue -> {
                    // If asking "less than", add "OR FIELD IS NULL" to read NULL values as 0, so accept the filter
                    Column column = getPopulationFrequencyColumn(keyOpValue[0]);
                    if (keyOpValue[1].startsWith(">") && !DEFAULT_HUMAN_POPULATION_FREQUENCIES_COLUMNS.contains(column)) {
                        return " OR \"" + column.column() + "\"[1] IS NULL";
                    }
                    return "";
                }, 1);

        addQueryFilter(query, ANNOT_GENE_TRAIT_ID, VariantColumn.XREFS, filters);

        addQueryFilter(query, ANNOT_GENE_TRAIT_NAME, VariantColumn.GENE_TRAITS_NAME, filters);

        addQueryFilter(query, ANNOT_HPO, VariantColumn.XREFS, filters);

        if (isValidParam(query, ANNOT_GO_GENES)) {
            String value = query.getString(ANNOT_GO_GENES.key());
            if (checkOperator(value) == QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_GO, value, "Unimplemented AND operator");
            }
            List<String> genesByGo = splitValue(value, QueryOperation.OR);
            if (genesByGo.isEmpty()) {
                // If any gene was found, the query will return no results.
                // FIXME: Find another way of returning empty results
                filters.add(getVoidFilter());
            } else {
                addQueryFilter(new Query(ANNOT_GO.key(), genesByGo), ANNOT_GO, VariantColumn.GENES, filters);
            }

        }
        if (isValidParam(query, ANNOT_EXPRESSION_GENES)) {
            String value = query.getString(ANNOT_EXPRESSION.key());
            if (checkOperator(value) == QueryOperation.AND) {
                throw VariantQueryException.malformedParam(VariantQueryParam.ANNOT_EXPRESSION, value, "Unimplemented AND operator");
            }
            List<String> genesByExpression = splitValue(value, QueryOperation.OR);
            if (genesByExpression.isEmpty()) {
                // If any gene was found, the query will return no results.
                // FIXME: Find another way of returning empty results
                filters.add(getVoidFilter());
            } else {
                addQueryFilter(new Query(ANNOT_EXPRESSION.key(), genesByExpression), ANNOT_EXPRESSION, VariantColumn.GENES, filters);
            }
        }

        addQueryFilter(query, ANNOT_PROTEIN_KEYWORD, VariantColumn.PROTEIN_KEYWORDS, filters);

        addQueryFilter(query, ANNOT_DRUG, VariantColumn.DRUG, filters);

        addQueryFilter(query, ANNOT_FUNCTIONAL_SCORE,
                (keyOpValue, rawValue) -> getFunctionalScoreColumn(keyOpValue[0], rawValue), null, filters);

        addQueryFilter(query, ANNOT_CLINICAL_SIGNIFICANCE, VariantColumn.CLINICAL_SIGNIFICANCE, filters);
    }

    private void addSoFlagCombination(Query query, List<String> filters) {
        List<String> soList = VariantQueryUtils.parseConsequenceTypes(query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key()));
        List<String> flags = query.getAsStringList(ANNOT_TRANSCRIPT_FLAG.key());
        List<String> combined = new ArrayList<>(soList.size() + flags.size());
        for (String so : soList) {
            int soNumber = parseConsequenceType(so);
            for (String flag : flags) {
                combined.add(VariantAnnotationToPhoenixConverter.combine(soNumber, flag));
            }
        }
        addQueryFilter(QueryParam.create("so_flag", "so+flag combination", Type.TEXT),
                VariantColumn.SO_FLAG, QueryOperation.OR, combined, filters);
    }

    private void addSoBiotypeCombination(Query query, List<String> filters) {
        List<String> soList = VariantQueryUtils.parseConsequenceTypes(query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key()));
        List<String> biotypes = query.getAsStringList(ANNOT_BIOTYPE.key());
        List<String> combined = new ArrayList<>(soList.size() + biotypes.size());
        for (String so : soList) {
            int soNumber = parseConsequenceType(so);
            for (String biotype : biotypes) {
                combined.add(VariantAnnotationToPhoenixConverter.combine(biotype, soNumber));
            }
        }
        addQueryFilter(QueryParam.create("so_biotype", "so+biotype combination", Type.TEXT),
                VariantColumn.BIOTYPE_SO, QueryOperation.OR, combined, filters);
    }

    /**
     * @return a filter which does not match with any chromosome.
     */
    private String getVoidFilter() {
        return buildFilter(VariantColumn.CHROMOSOME, "=", "_VOID");
    }

    protected void addStatsFilters(ParsedVariantQuery variantQuery, List<String> filters) {
        StudyMetadata defaultStudyMetadata = variantQuery.getStudyQuery().getDefaultStudy();
        Query query = variantQuery.getQuery();
        List<Integer> allStudies = metadataManager.getStudyIds();
        Set<Integer> studiesFilter;
        QueryOperation studyOp;
        if (VariantQueryUtils.isValidParam(query, STUDY, true)) {
            studiesFilter = VariantQueryUtils.splitValue(query, STUDY).getValues()
                    .stream()
                    .filter(s -> !isNegated(s))
                    .map(metadataManager::getStudyId)
                    .collect(Collectors.toSet());
            if (studiesFilter.size() == 1) {
                studyOp = null;
            } else {
                studyOp = VariantQueryUtils.checkOperator(query.getString(STUDY.key()));
            }

        } else {
            // any study
            studiesFilter = new HashSet<>(allStudies);
            if (studiesFilter.size() == 1) {
                studyOp = null;
            } else {
                studyOp = QueryOperation.OR;
            }
        }

        addQueryFilter(query, STATS_REF,
                (keyOpValue, v) -> getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsFreqColumn),
                null,
                filters,
                keyOpValue -> {
                    if (keyOpValue[1].equals(">") || keyOpValue[1].equals(">=")) {
                        Column column = getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsFreqColumn);
                        Integer studyId = VariantPhoenixHelper.extractStudyId(column.column(), true);

                        if (!studiesFilter.contains(studyId) || studyOp == QueryOperation.OR) {
                            return " OR \"" + column.column() + "\"[1] IS NULL ";
                        }
                    }
                    return "";
                },
                null,
                s -> 1);

        addQueryFilter(query, STATS_ALT,
                (keyOpValue, v) -> getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsFreqColumn),
                null, filters,
                keyOpValue -> {
                    if (keyOpValue[1].equals("<") || keyOpValue[1].equals("<=")) {
                        Column column = getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsFreqColumn);
                        Integer studyId = VariantPhoenixHelper.extractStudyId(column.column(), true);

                        if (!studiesFilter.contains(studyId) || studyOp == QueryOperation.OR) {
                            return " OR \"" + column.column() + "\"[2] IS NULL ";
                        }
                    }
                    return "";
                }, null, s -> 2);

        addQueryFilter(query, STATS_MAF,
                (keyOpValue, v) -> getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsMafColumn),
                null, filters,
                keyOpValue -> {
                    if (keyOpValue[1].equals("<") || keyOpValue[1].equals("<=")) {
                        Column column = getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsMafColumn);
                        Integer studyId = VariantPhoenixHelper.extractStudyId(column.column(), true);

                        if (!studiesFilter.contains(studyId) || studyOp == QueryOperation.OR) {
                            return " OR \"" + column.column() + "\" IS NULL ";
                        }
                    }
                    return "";
                }, null, null);

        addQueryFilter(query, STATS_MGF,
                (keyOpValue, v) -> getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsMgfColumn),
                null, filters,
                keyOpValue -> {
                    if (keyOpValue[1].equals("<") || keyOpValue[1].equals("<=")) {
                        Column column = getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsMgfColumn);
                        Integer studyId = VariantPhoenixHelper.extractStudyId(column.column(), true);

                        if (!studiesFilter.contains(studyId) || studyOp == QueryOperation.OR) {
                            return " OR \"" + column.column() + "\" IS NULL ";
                        }
                    }
                    return "";
                }, null, null);

        addQueryFilter(query, STATS_PASS_FREQ,
                (String[] keyOpValue, String v) -> getCohortColumn(keyOpValue, defaultStudyMetadata,
                        VariantPhoenixHelper::getStatsPassFreqColumn),
                null, filters,
                (String[] keyOpValue) -> {
                    if (keyOpValue[1].equals("<") || keyOpValue[1].equals("<=")) {
                        Column column = getCohortColumn(keyOpValue, defaultStudyMetadata, VariantPhoenixHelper::getStatsPassFreqColumn);
                        Integer studyId = VariantPhoenixHelper.extractStudyId(column.column(), true);

                        if (!studiesFilter.contains(studyId) || studyOp == QueryOperation.OR) {
                            return " OR \"" + column.column() + "\"[2] IS NULL ";
                        }
                    }
                    return "";
                }, -1);

        unsupportedFilter(query, MISSING_ALLELES);

        unsupportedFilter(query, MISSING_GENOTYPES);
    }

    private Column getCohortColumn(String[] keyOpValue, StudyMetadata defaultMetadata, BiFunction<Integer, Integer, Column> columnBuilder) {
        String key = keyOpValue[0];
        String[] split = VariantQueryUtils.splitStudyResource(key);

        String cohort;
        final StudyMetadata sm;
        if (split.length == 2) {
            String study = split[0];
            cohort = split[1];
            sm = metadataManager.getStudyMetadata(study);
        } else {
            cohort = key;
            sm = defaultMetadata;
        }
        Integer cohortId = metadataManager.getCohortId(sm.getId(), cohort);
        if (cohortId == null) {
            throw VariantQueryException.cohortNotFound(cohort, sm.getId(), metadataManager);
        }

        return columnBuilder.apply(sm.getId(), cohortId);
    }


    private void addQueryFilter(Query query, QueryParam param, Column column, List<String> filters) {
        addQueryFilter(query, param, column, filters, null);
    }

    private void addQueryFilter(Query query, QueryParam param, Column column, List<String> filters,
                                Function<String, Object> valueParser) {
        addQueryFilter(query, param, (a, s) -> column, valueParser, filters, null, -1);
    }

    private void addQueryFilter(Query query, QueryParam param, BiFunction<String[], String, Column> columnParser,
                                Function<String, Object> valueParser, List<String> filters) {
        addQueryFilter(query, param, columnParser, valueParser, filters, null, -1);
    }

    /**
     * Transforms a Key-Value from a query into a valid SQL filter.
     *
     * @param query             Query with the values
     * @param param             Param to read from the query
     * @param columnParser      Column parser. Given the [key, op, value] and the original value, returns a {@link Column}
     * @param valueParser       Value parser. Given the [key, op, value], transforms the value to make the query.
     *                          If the returned value is a Collection, uses each value for the query.
     * @param filters           List of filters to be modified.
     * @param extraFilters      Provides extra filters to be concatenated to the filter.
     * @param arrayIdx          Array accessor index in base-1.
     */
    private void addQueryFilter(Query query, QueryParam param,
                                BiFunction<String[], String, Column> columnParser,
                                Function<String, Object> valueParser,
                                List<String> filters, Function<String[], String> extraFilters, int arrayIdx) {
        addQueryFilter(query, param, columnParser, valueParser, filters, extraFilters, null, (o) -> arrayIdx);
    }

    /**
     * Transforms a Key-Value from a query into a valid SQL filter.
     *
     * @param query             Query with the values
     * @param param             Param to read from the query
     * @param columnParser      Column parser. Given the [key, op, value] and the original value, returns a {@link Column}
     * @param valueParser       Value parser. Given the [key, op, value], transforms the value to make the query.
     *                          If the returned value is a Collection, uses each value for the query.
     * @param filters           List of filters to be modified.
     * @param extraFilters      Provides extra filters to be concatenated to the filter.
     * @param operatorParser    Operator parser. Given the [key, op, value], returns a valid SQL operator
     * @param arrayIdxParser    Array accessor index in base-1.
     */
    private void addQueryFilter(Query query, QueryParam param,
                                BiFunction<String[], String, Column> columnParser,
                                Function<String, Object> valueParser,
                                List<String> filters,
                                Function<String[], String> extraFilters,
                                Function<String, String> operatorParser,
                                Function<String, Integer> arrayIdxParser) {
        if (isValidParam(query, param)) {
            String stringValue = query.getString(param.key());
            QueryOperation logicOperation = checkOperator(stringValue);
            addQueryFilter(param, columnParser, valueParser, filters, extraFilters, operatorParser, arrayIdxParser, logicOperation,
                    splitValue(stringValue, logicOperation));
        }
    }


    private void addQueryFilter(QueryParam param,
                                Column column,
                                QueryOperation logicOperation,
                                List<String> rawValues,
                                List<String> filters) {
        addQueryFilter(param, (a, b) -> column, null, filters, null, null, null, logicOperation, rawValues);
    }

    private void addQueryFilter(QueryParam param,
                                BiFunction<String[], String, Column> columnParser,
                                Function<String, Object> valueParser,
                                List<String> filters,
                                Function<String[], String> extraFilters,
                                Function<String, String> operatorParser,
                                Function<String, Integer> arrayIdxParser,
                                QueryOperation logicOperation,
                                List<String> rawValues) {
        List<String> subFilters = new LinkedList<>();
        if (logicOperation == null) {
            logicOperation = QueryOperation.AND;
        }

        for (String rawValue : rawValues) {
            String[] keyOpValue = splitOperator(rawValue);
            Column column = columnParser.apply(keyOpValue, rawValue);


//                String op = parseOperator(keyOpValue[1]);
            String op = keyOpValue[1];
            if (operatorParser != null) {
                op = operatorParser.apply(op);
            }
            int arrayIdx = arrayIdxParser == null ? -1 : arrayIdxParser.apply(op);

            if (!column.getPDataType().isArrayType() && arrayIdx >= 0) {
                throw new VariantQueryException("Unable to use array indexes with non array columns. "
                        + column + " " + column.sqlType());
            }

            final String negatedStr;
            boolean negated = false;
            if (op.startsWith("!")) {
                op = inverseOperator(op);
                negated = true;
                negatedStr = "NOT ";
            } else {
                negatedStr = "";
            }

            String extra = "";
            if (extraFilters != null) {
                extra = extraFilters.apply(keyOpValue);
            }

            if (valueParser != null) {
                Object value = valueParser.apply(keyOpValue[2]);
                if (value instanceof Collection) {
                    List<String> subSubFilters = new ArrayList<>(((Collection) value).size());
                    for (Object o : ((Collection) value)) {
                        subSubFilters.add(buildFilter(column, op, o.toString(), "", extra, arrayIdx, param, rawValue));
                    }
                    subFilters.add(negatedStr + appendFilters(subSubFilters, QueryOperation.OR));
                } else {
                    subFilters.add(buildFilter(column, op, value.toString(), negatedStr, extra, arrayIdx, param, rawValue));
                }
            } else {
                subFilters.add(buildFilter(column, op, keyOpValue[2], negatedStr, extra, arrayIdx, param, rawValue));
            }
        }
        filters.add(appendFilters(subFilters, logicOperation));
//        filters.add(subFilters.stream().collect(Collectors.joining(" ) " + operation.name() + " ( ", " ( ", " ) ")));
    }

    private String buildFilter(Column column, String op, Object value) {
        return buildFilter(column, op, value, "", "", 0, null, null);
    }

    private String buildFilter(Column column, String op, Object value, boolean negated) {
        return buildFilter(column, op, value, negated ? "NOT " : "", "", 0, null, null);
    }


    private String buildFilter(Column column, String op, Object value, String negated, String extra, int idx,
                               QueryParam param, String rawValue) {
        Object parsedValue;
        StringBuilder sb = new StringBuilder();

        String arrayPosition = "";

        if (StringUtils.isNotEmpty(extra)) {
            sb.append("( ");
        }
        String sqlType = column.sqlType();
        if (idx > 0) {
            sqlType = sqlType.replace(" ARRAY", "");
            arrayPosition = "[" + idx + "]";
        }
        boolean orNull = op.startsWith(">>") || op.startsWith("<<");
        switch (sqlType) {
            case "VARCHAR":
                parsedValue = checkStringValue((String) value);
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ');
                if (((String) parsedValue).isEmpty()) {
                    sb.append("IS NULL");
                } else {
                    sb.append(parseOperator(op))
                            .append(" '").append(parsedValue).append('\'');
                }
                break;
            case "VARCHAR ARRAY":
                parsedValue = checkStringValue((String) value);
                checkStringValue((String) parsedValue);
                sb.append(negated)
                        .append('\'').append(parsedValue).append("' ")
                        .append(parseOperator(op))
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "INTEGER ARRAY":
                parsedValue = parseInteger(value, param, rawValue);
                String operator = flipOperator(parseNumericOperator(op));
                sb.append(negated)
                        .append(parsedValue).append(' ')
                        .append(operator)
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "INTEGER":
            case "UNSIGNED_INT":
                parsedValue = parseInteger(value, param, rawValue);
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ')
                        .append(parseNumericOperator(op))
                        .append(' ').append(parsedValue);
                break;
            case "FLOAT ARRAY":
            case "DOUBLE ARRAY":
                parsedValue = parseDouble(value, param, rawValue);
                String flipOperator = flipOperator(parseNumericOperator(op));
                sb.append(negated)
                        .append(parsedValue).append(' ')
                        .append(flipOperator)
                        .append(" ANY(\"").append(column).append("\")");
                break;
            case "FLOAT":
            case "DOUBLE":
                parsedValue = parseDouble(value, param, rawValue);
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ')
                        .append(parseNumericOperator(op))
                        .append(' ').append(parsedValue);
                break;
            case "BOOLEAN":
                parsedValue = parseBoolean(value);
                sb.append(negated)
                        .append('"').append(column).append('"').append(arrayPosition).append(' ')
                        .append(parseBooleanOperator(op))
                        .append(' ').append(parsedValue);
                break;
            default:
                throw new VariantQueryException("Unsupported column type " + column.getPDataType().getSqlTypeName()
                        + " for column " + column);
        }
        if (orNull) {
            sb.append(" OR \"").append(column).append("\" IS NULL");
        }
        if (StringUtils.isNotEmpty(extra)) {
            sb.append(' ').append(extra).append(" )");
        }
        return sb.toString();
    }

    private double parseDouble(Object value, QueryParam param, String rawValue) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else {
            try {
                return Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
                if (param != null) {
                    throw VariantQueryException.malformedParam(param, rawValue);
                } else {
                    throw new VariantQueryException("Error parsing decimal value '" + value + '\'', e);
                }
            }
        }
    }

    private int parseInteger(Object value, QueryParam param, String rawValue) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else {
            try {
                return Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                if (param != null) {
                    throw VariantQueryException.malformedParam(param, rawValue);
                } else {
                    throw new VariantQueryException("Error parsing integer value '" + value + '\'', e);
                }
            }
        }
    }

    private boolean parseBoolean(Object value) {
        if (value instanceof Boolean) {
            return ((Boolean) value);
        } else {
            return Boolean.parseBoolean(value.toString());
        }
    }

    private String checkStringValue(String value) {
        if (value == null) {
            throw new VariantQueryException("Unable to query null text field");
        }
        if (value.contains("'")) {
            throw new VariantQueryException("Unable to query text field with \"'\" : " + value);
        }
        return value;
    }

    /**
     * Flip the operator to flip the order of the operands.
     *
     * ">" --> "<"
     * "<" --> ">"
     * ">=" --> "<="
     * "<=" --> ">="
     *
     * @param op    Operation to flip
     * @return      Operation flipped
     */
    public static String flipOperator(String op) {
        StringBuilder sb = new StringBuilder(op.length());
        for (int i = 0; i < op.length(); i++) {
            char c = op.charAt(i);
            if (c == '>') {
                c = '<';
            } else if (c == '<') {
                c = '>';
            }
            sb.append(c);
        }
        return sb.toString();
//        return op.replace(">", "G").replace("<", ">").replace("G", "<");
    }

    /**
     * Inverse the operator obtaining the opposite operator.
     *
     * ">" --> "<="
     * "<" --> ">="
     * ">=" --> "<"
     * "<=" --> ">"
     *
     * @param op    Operation to inverse
     * @return      Operation inverted
     */
    public static String inverseOperator(String op) {
        switch (op) {
            case ">":
                return "<=";
            case ">>":
                return "<<=";
            case ">=":
                return "<";
            case ">>=":
                return "<<";
            case "<":
                return ">=";
            case "<<":
                return ">>=";
            case "<=":
                return ">";
            case "<<=":
                return ">>";
            case "":
            case "=":
            case "==":
                return "!=";
            case "!":
            case "!=":
                return "=";
            default:
                throw new VariantQueryException("Unknown operator " + op);
        }
    }

    public static String parseOperator(String op) {
        return SQL_OPERATOR.getOrDefault(op, op);
    }

    public static String parseNumericOperator(String op) {
        String parsedOp = parseOperator(op);
        if (parsedOp.equals("LIKE")) {
            throw new VariantQueryException("Unable to use REGEX operator (" + op + ") with numerical fields");
        }
        return parsedOp;
    }

    public static String parseBooleanOperator(String op) {
        String parsedOp = parseOperator(op);
        if (!parsedOp.equals("=") && !parsedOp.equals("!=")) {
            throw new VariantQueryException("Unable to use operator (" + op + ") with boolean fields");
        }
        return parsedOp;
    }


}
