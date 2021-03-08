package org.opencb.opencga.storage.hadoop.variant.adaptors.sample;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleDataManager;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.SAMPLE_DATA_SUFIX;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.buildStudyColumnsPrefix;

public class HBaseVariantSampleDataManager extends VariantSampleDataManager {

    private final VariantHadoopDBAdaptor dbAdaptor;
    private final VariantStorageMetadataManager metadataManager;
    private final CellBaseUtils cellBaseUtils;

    public HBaseVariantSampleDataManager(VariantHadoopDBAdaptor dbAdaptor, CellBaseUtils cellBaseUtils) {
        super(dbAdaptor);
        this.dbAdaptor = dbAdaptor;
        metadataManager = dbAdaptor.getMetadataManager();
        this.cellBaseUtils = cellBaseUtils;
    }

    @Override
    protected DataResult<Variant> getSampleData(String variantStr, String study, QueryOptions options,
                                                List<String> includeSamples,
                                                Set<String> genotypes,
                                                int sampleLimit) {
        StopWatch stopWatch = StopWatch.createStarted();
        Set<VariantField> includeFields = VariantField.getIncludeFields(options);

        final Variant variant;
        if (VariantQueryUtils.isVariantId(variantStr)) {
            variant = new Variant(variantStr);
        } else {
            variant = cellBaseUtils.getVariant(variantStr);
        }
        variant.setId(variant.toString());

        int studyId = metadataManager.getStudyId(study);

        boolean includeNoneSamples = VariantQueryUtils.isNoneOrEmpty(includeSamples);
        if (includeNoneSamples) {
            throw new VariantQueryException("Unable to continue including none sample");
        }
        boolean includeAllSamples = VariantQueryUtils.isAllOrNull(includeSamples);
        Set<Integer> includeSampleIds = new HashSet<>();
        if (!includeAllSamples) {
            for (String sample : includeSamples) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, study);
                }
                includeSampleIds.add(sampleId);
            }
        }

        int skip = Math.max(0, options.getInt(QueryOptions.SKIP, 0));
        int limit = Math.max(0, options.getInt(QueryOptions.LIMIT, 10));

        try {
            List<Integer> samples = new ArrayList<>(limit);
            List<VariantRow.SampleColumn> sampleDataMap = new ArrayList<>(limit);

            dbAdaptor.getHBaseManager().act(dbAdaptor.getVariantTable(), table -> {
                // Create one GET for samples
                Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
                LinkedList<Filter> filters = new LinkedList<>();

                filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryPrefixComparator(Bytes.toBytes(buildStudyColumnsPrefix(studyId)))));
                // Filter columns by sample sufix
                filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(buildStudyColumnsPrefix(studyId) + "[0-9_]*" + SAMPLE_DATA_SUFIX)));

                LinkedList<Filter> genotypeFilters = new LinkedList<>();
                for (String genotype : genotypes) {
                    byte[] gtBytes;
                    if (genotype.equals(GenotypeClass.NA_GT_VALUE)) {
                        gtBytes = new byte[]{0};
                    } else {
                        gtBytes = new byte[genotype.length() + 1];
                        System.arraycopy(Bytes.toBytes(genotype), 0, gtBytes, 0, genotype.length());
                    }
                    genotypeFilters.add(new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(gtBytes)));
                }
                if (genotypeFilters.size() == 1) {
                    filters.add(genotypeFilters.getFirst());
                } else {
                    filters.add(new FilterList(FilterList.Operator.MUST_PASS_ONE, genotypeFilters));
                }

                filters.add(new ColumnPaginationFilter(limit, skip));
                get.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters));
                if (!includeAllSamples) {
                    for (Integer sampleId : includeSampleIds) {
                        byte[] column = VariantPhoenixSchema.buildSampleColumnKey(studyId, sampleId);
                        get.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, column);
                    }
                }
                Result result = table.get(get);
                if (!result.isEmpty()) {
                    VariantRow.walker(result)
                            .onSample(sampleColumn -> {
                                if (sampleColumn.getStudyId() == studyId) {
                                    samples.add(sampleColumn.getSampleId());
                                    sampleDataMap.add(sampleColumn);
                                }
                            }).walk();
                }
            });

            // Query files, stats and annotation
            List<Pair<String, PhoenixArray>> filesMap = new ArrayList<>();
            Set<Integer> fileIdsFromSampleIds = metadataManager.getFileIdsFromSampleIds(studyId, samples);
            HBaseToVariantStatsConverter statsConverter = new HBaseToVariantStatsConverter();
            List<VariantStats> stats = new LinkedList<>();
            dbAdaptor.getHBaseManager().act(dbAdaptor.getVariantTable(), table -> {
                Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
                // Add file columns
                for (Integer fileId : fileIdsFromSampleIds) {
                    get.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.buildFileColumnKey(studyId, fileId));
                }

                // Add Stats column
                if (includeFields.contains(VariantField.STUDIES_STATS)) {
                    Integer cohortId = metadataManager.getCohortId(studyId, StudyEntry.DEFAULT_COHORT);
                    PhoenixHelper.Column statsColumn = VariantPhoenixSchema.getStatsColumn(studyId, cohortId);
                    get.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, statsColumn.bytes());
                }

                // Add annotation column
                if (includeFields.contains(VariantField.ANNOTATION)) {
                    PhoenixHelper.Column annotationColumn = VariantPhoenixSchema.VariantColumn.FULL_ANNOTATION;
                    get.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, annotationColumn.bytes());
                }

                // Get
                Result result = table.get(get);

                if (result == null || result.isEmpty()) {
                    throw VariantQueryException.variantNotFound(variantStr);
                }
                // Walk row
                VariantRow.walker(result)
                        .onFile(fileColumn -> {
                            filesMap.add(Pair.of(String.valueOf(fileColumn.getFileId()), fileColumn.raw()));
                        })
                        .onCohortStats(statsCell -> {
                            VariantStats variantStats = statsConverter.convert(statsCell);
                            variantStats.setCohortId(metadataManager.getCohortName(studyId, statsCell.getCohortId()));
                            stats.add(variantStats);
                        })
                        .onVariantAnnotation(column -> {
                            ImmutableBytesWritable b = column.toBytesWritable();
                            variant.setAnnotation(new HBaseToVariantAnnotationConverter().convert(b.get(), b.getOffset(), b.getLength()));
                        })
                        .walk();
            });

            // Convert to VariantSampleData
            HBaseToStudyEntryConverter converter = new HBaseToStudyEntryConverter(metadataManager, statsConverter);

            converter.configure(HBaseVariantConverterConfiguration.builder()
                    .setStudyNameAsStudyId(true)
                    .setIncludeSampleId(true)
                    .setProjection(
                            new VariantQueryProjection(
                                    metadataManager.getStudyMetadata(studyId),
                                    samples,
                                    new ArrayList<>(fileIdsFromSampleIds)))
                    .build());

            StudyEntry studyEntry = converter.convert(sampleDataMap, filesMap, variant, studyId);

            variant.addStudyEntry(studyEntry);
            studyEntry.setStats(stats);
//        String msg = "Queries : " + queries + " , readSamples : " + readSamples;
            return new DataResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), 1,
                    Collections.singletonList(variant), 1);
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected Map<String, Collection<String>> getGenotypeGroups(int studyId, Set<String> genotypes, boolean merge) {
        Map<String, Collection<String>> gtGroups = new LinkedHashMap<>(genotypes.size());
        List<String> loadedGts = metadataManager.getStudyMetadata(studyId).getAttributes()
                .getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key());
        if (merge) {
            List<String> allGts = new LinkedList<>();
            for (String genotypeStr : genotypes) {
                allGts.add(genotypeStr);
                if (!genotypeStr.equals(GenotypeClass.NA_GT_VALUE)) {
                    Genotype genotype = new Genotype(genotypeStr);
                    allGts.addAll(GenotypeClass.getPhasedGenotypes(genotype, loadedGts));
                }
            }
            gtGroups.put(VariantQueryUtils.ALL, allGts);
        } else {
            for (String genotypeStr : genotypes) {
                List<String> allGts = new ArrayList<>(3);
                allGts.add(genotypeStr);
                if (!genotypeStr.equals(GenotypeClass.NA_GT_VALUE)) {
                    Genotype genotype = new Genotype(genotypeStr);
                    allGts.addAll(GenotypeClass.getPhasedGenotypes(genotype, loadedGts));
                }
                gtGroups.put(genotypeStr, allGts);
            }
        }
        return gtGroups;
    }
}
