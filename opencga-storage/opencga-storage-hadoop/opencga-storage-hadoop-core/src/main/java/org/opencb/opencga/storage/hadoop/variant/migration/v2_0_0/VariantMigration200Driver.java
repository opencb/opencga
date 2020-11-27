package org.opencb.opencga.storage.hadoop.variant.migration.v2_0_0;

import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.*;

public class VariantMigration200Driver extends AbstractVariantsTableDriver {

    public static final String REMOVE_SPAN_DELETIONS = "remove_span_deletions";

    private static final String VARIANT_TYPE_MIGRATION_MAPPER_VARIANTS_TABLE = "VariantTypeMigrationMapper.variantsTable";
    private static final String VARIANT_TYPE_MIGRATION_MAPPER_DELETED_VARIANTS_TABLE = "VariantTypeMigrationMapper.deletedVariantsTable";
    private final Logger logger = LoggerFactory.getLogger(VariantMigration200Driver.class);
    private String region = null;

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("--" + REMOVE_SPAN_DELETIONS, "<true|false>");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        region = getConf().get(VariantQueryParam.REGION.key());

        String deletedSpanDeletionsTable = getVariantsTable() + "_old_span_del";
        List<byte[]> splitList = GenomeHelper.generateBootPreSplitsHuman(
                50,
                VariantPhoenixKeyFactory::generateVariantRowKey);
        getHBaseManager()
                .createTableIfNeeded(deletedSpanDeletionsTable, GenomeHelper.COLUMN_FAMILY_BYTES, splitList, Compression.Algorithm.GZ);

        getConf().set(VARIANT_TYPE_MIGRATION_MAPPER_VARIANTS_TABLE, getVariantsTable());
        getConf().set(VARIANT_TYPE_MIGRATION_MAPPER_DELETED_VARIANTS_TABLE, deletedSpanDeletionsTable);
        if (getConf().getBoolean(REMOVE_SPAN_DELETIONS, false)) {
            logger.info("Remove span deletions from main variants table");
        } else {
            logger.info("Do not remove span deletions from main variants table");
        }


    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(region)) {
            logger.info("Execute migration for region " + region);
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        } else {
            logger.info("Execute migration for the whole table");
        }
//        scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
//                new ColumnPrefixFilter(VariantPhoenixHelper.VariantColumn.TYPE.bytes()),
//                new ColumnPrefixFilter()));
        scan = VariantMapReduceUtil.configureMapReduceScan(scan, getConf());
        VariantMapReduceUtil.initTableMapperMultiOutputJob(job, variantTable,
                Collections.singletonList(scan), VariantTypeMigrationMapper.class);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "variant-type-migration-2.0.0";
    }

    public static class VariantTypeMigrationMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

        private ImmutableBytesWritable variantsTable;
        private ImmutableBytesWritable deletedVariantsTable;
        private boolean deleteSpanDeletions;
        private final Logger logger = LoggerFactory.getLogger(VariantTypeMigrationMapper.class);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            variantsTable = new ImmutableBytesWritable(
                    Bytes.toBytes(context.getConfiguration().get(VARIANT_TYPE_MIGRATION_MAPPER_VARIANTS_TABLE)));
            deletedVariantsTable = new ImmutableBytesWritable(
                    Bytes.toBytes(context.getConfiguration().get(VARIANT_TYPE_MIGRATION_MAPPER_DELETED_VARIANTS_TABLE)));
            deleteSpanDeletions = context.getConfiguration().getBoolean(REMOVE_SPAN_DELETIONS, false);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            VariantRow variant = new VariantRow(result);
            Put put = new Put(result.getRow());
            boolean isSpanDeletion = variant.getVariant().getAlternate().equals(Allele.SPAN_DEL_STRING);
            final boolean[] isMainVariant = {false};
            final int[] validSecondaryAlternates = {0};

            variant.walk(new VariantRow.VariantRowWalker() {
                @Override
                protected void type(VariantType type) {
                    VariantType newType = null;
                    if (type == VariantType.SNP) {
                        newType = VariantType.SNV;
                    } else if (type == VariantType.MNP) {
                        newType = VariantType.MNV;
                    } else if (type == VariantType.CNV) {
                        newType = VariantType.COPY_NUMBER;
                        StructuralVariation sv = variant.getVariant().getSv();
                        if (sv.getType().equals(StructuralVariantType.COPY_NUMBER_GAIN)) {
                            newType = VariantType.COPY_NUMBER_GAIN;
                        } else if (sv.getType().equals(StructuralVariantType.COPY_NUMBER_LOSS)) {
                            newType = VariantType.COPY_NUMBER_LOSS;
                        }
                    } else if (type == VariantType.DUPLICATION) {
                        StructuralVariation sv = variant.getVariant().getSv();
                        if (sv.getType().equals(StructuralVariantType.TANDEM_DUPLICATION)) {
                            newType = VariantType.TANDEM_DUPLICATION;
                        }
                    }
                    if (newType != null) {
                        put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES,
                                VariantPhoenixHelper.VariantColumn.TYPE.bytes(), Bytes.toBytes(newType.name()));
                    }
                }

                @Override
                protected void file(VariantRow.FileColumn fileColumn) {
                    OriginalCall call = fileColumn.getCall();
                    if (call == null || call.getAlleleIndex().equals(0)) {
                        isMainVariant[0] = true;
                    }

                    List<AlternateCoordinate> secondaryAlternates = fileColumn.getSecondaryAlternates();

                    if (secondaryAlternates != null) {
                        boolean modified = false;
                        for (AlternateCoordinate secAlt : secondaryAlternates) {
                            VariantType type = secAlt.getType();
                            if (type == VariantType.SNP) {
                                secAlt.setType(VariantType.SNV);
                                modified = true;
                            } else if (type == VariantType.MNP) {
                                secAlt.setType(VariantType.MNV);
                                modified = true;
                            } else if (type == VariantType.CNV) {
                                secAlt.setType(VariantType.COPY_NUMBER);
                                modified = true;
                            } else if (type == VariantType.MIXED) {
                                if (secAlt.getAlternate().equals(Allele.SPAN_DEL_STRING)) {
                                    secAlt.setType(VariantType.DELETION);
                                }
                                modified = true;
                            }
                            if (!secAlt.getAlternate().equals(VariantBuilder.SPAN_DELETION) && type != VariantType.NO_VARIATION) {
                                validSecondaryAlternates[0]++;
                            }
                        }
                        if (modified) {
                            String[] values;
                            try {
                                values = (String[]) fileColumn.raw().getArray();
                            } catch (SQLException e) {
                                // This can not happen.
                                throw new IllegalStateException(e);
                            }
                            values[HBaseToStudyEntryConverter.FILE_SEC_ALTS_IDX]
                                    = StudyEntryToHBaseConverter.getSecondaryAlternates(variant.getVariant(), secondaryAlternates);
                            put.addColumn(
                                    GenomeHelper.COLUMN_FAMILY_BYTES,
                                    VariantPhoenixHelper.buildFileColumnKey(fileColumn.getStudyId(), fileColumn.getFileId()),
                                    PhoenixHelper.toBytes(values, PVarcharArray.INSTANCE)
                            );
                        }
                    }
                }
            });

            context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
            if (isSpanDeletion) {
                context.getCounter(COUNTER_GROUP_NAME, "span_deletion").increment(1);
                if (isMainVariant[0]) {
                    context.getCounter(COUNTER_GROUP_NAME, "span_deletion_main").increment(1);
                }
                if (validSecondaryAlternates[0] == 0) {
                    context.getCounter(COUNTER_GROUP_NAME, "span_deletion_alone").increment(1);
                    logger.info("span_deletion_alone : " + variant.getVariant().toString());
                }
                Put copyResult = new Put(result.getRow());
                for (Cell cell : result.rawCells()) {
                    copyResult.add(cell);
                }
                context.getCounter(COUNTER_GROUP_NAME, "moved_span_del_rows").increment(1);
                context.write(deletedVariantsTable, copyResult);

                if (deleteSpanDeletions) {
                    Delete delete = new Delete(result.getRow()).addFamily(GenomeHelper.COLUMN_FAMILY_BYTES);
                    context.write(variantsTable, delete);
                    context.getCounter(COUNTER_GROUP_NAME, "span_del_deleted").increment(1);
                } else {
                    context.getCounter(COUNTER_GROUP_NAME, "span_del_deletion_skipped").increment(1);
                }
            } else {
                if (put.isEmpty()) {
                    context.getCounter(COUNTER_GROUP_NAME, "ok").increment(1);
                } else {
                    context.getCounter(COUNTER_GROUP_NAME, "fixes").increment(1);
                    context.write(variantsTable, put);
                }
            }

        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }

}
