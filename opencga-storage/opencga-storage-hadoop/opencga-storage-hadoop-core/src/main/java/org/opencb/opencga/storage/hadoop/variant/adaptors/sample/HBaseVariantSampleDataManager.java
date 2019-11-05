package org.opencb.opencga.storage.hadoop.variant.adaptors.sample;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryFields;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.sample.SampleData;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleData;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleDataManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.SAMPLE_DATA_SUFIX;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper.buildStudyColumnsPrefix;

public class HBaseVariantSampleDataManager extends VariantSampleDataManager {

    private final VariantHadoopDBAdaptor dbAdaptor;
    private final VariantStorageMetadataManager metadataManager;

    public HBaseVariantSampleDataManager(VariantHadoopDBAdaptor dbAdaptor) {
        super(dbAdaptor);
        this.dbAdaptor = dbAdaptor;
        metadataManager = dbAdaptor.getMetadataManager();
    }

    @Override
    protected DataResult<VariantSampleData> getSampleData(String variantStr, String study, QueryOptions options,
                                                           List<String> includeSamples,
                                                           boolean studyWithGts, Set<String> genotypes,
                                                           boolean merge, int sampleLimit) {
        StopWatch stopWatch = StopWatch.createStarted();

        Variant variant = new Variant(variantStr);

        int studyId = metadataManager.getStudyId(study);

        boolean includeAllSamples = CollectionUtils.isEmpty(includeSamples)
                || includeSamples.size() == 1 && VariantQueryUtils.ALL.equals(includeSamples.get(0));
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

        // Create GT groups
        Map<String, Collection<String>> gtGroups = getGenotypeGroups(studyId, genotypes, merge);
        Map<String, String> gtGroupsInverse = new HashMap<>();
        gtGroups.forEach((gt, subGts) -> subGts.forEach(subGt -> gtGroupsInverse.put(subGt, gt)));

        try {
            // Create one GET per group of genotypes
            List<Get> sampleGets = new ArrayList<>(gtGroups.size());
            for (Map.Entry<String, Collection<String>> entry : gtGroups.entrySet()) {
                Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
                LinkedList<Filter> filters = new LinkedList<>();

                filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryPrefixComparator(Bytes.toBytes(buildStudyColumnsPrefix(studyId)))));
                // Filter columns by sample sufix
                filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(buildStudyColumnsPrefix(studyId) + "[0-9]*" + SAMPLE_DATA_SUFIX)));

                if (studyWithGts) {
                    LinkedList<Filter> genotypeFilters = new LinkedList<>();
                    for (String genotype : entry.getValue()) {
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
                }

                filters.add(new ColumnPaginationFilter(limit, skip));
                get.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters));
                if (!includeAllSamples) {
                    for (Integer sampleId : includeSampleIds) {
                        byte[] column = VariantPhoenixHelper.buildSampleColumnKey(studyId, sampleId);
                        get.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, column);
                    }
                }
                sampleGets.add(get);
            }

            // Execute queries
            List<Integer> samples = new ArrayList<>(gtGroups.size() * limit);
            List<Pair<Integer, List<String>>> sampleDataMap = new ArrayList<>(gtGroups.size() * limit);
            dbAdaptor.getHBaseManager().act(dbAdaptor.getVariantTable(), table -> {
                Result[] results = table.get(sampleGets);

                for (int i = 0; i < gtGroups.size(); i++) {
                    Result result = results[i];
                    if (result == null) {
                        continue;
                    }
                    for (Cell cell : result.rawCells()) {
                        byte[] qualifier = CellUtil.cloneQualifier(cell);
                        if (cell.getValueLength() == 0) {
                            continue;
                        }
                        // Extract sample data
                        if (AbstractPhoenixConverter.endsWith(qualifier, VariantPhoenixHelper.SAMPLE_DATA_SUFIX_BYTES)) {
                            String columnName = Bytes.toString(qualifier);
                            String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                            if (Integer.valueOf(split[0]) != studyId) {
                                continue;
                            }
                            Integer sampleId = Integer.valueOf(split[1]);
                            samples.add(sampleId);
                            PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(
                                    cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                            List<String> sampleData = AbstractPhoenixConverter.toModifiableList(array);
                            sampleDataMap.add(Pair.of(sampleId, sampleData));
                        }
                    }
                }
            });

            // Query files and stats
            List<Pair<String, PhoenixArray>> filesMap = new ArrayList<>();
            Set<Integer> fileIdsFromSampleIds = metadataManager.getFileIdsFromSampleIds(studyId, samples);
            HBaseToVariantStatsConverter statsConverter = new HBaseToVariantStatsConverter(dbAdaptor.getGenomeHelper());
            Map<String, VariantStats> stats = new HashMap<>();
            dbAdaptor.getHBaseManager().act(dbAdaptor.getVariantTable(), table -> {
                Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
                // Add file columns
                for (Integer fileId : fileIdsFromSampleIds) {
                    get.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixHelper.buildFileColumnKey(studyId, fileId));
                }

                // Add Stats column
                Integer cohortId = metadataManager.getCohortId(studyId, StudyEntry.DEFAULT_COHORT);
                PhoenixHelper.Column statsColumn = VariantPhoenixHelper.getStatsColumn(studyId, cohortId);
                get.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, statsColumn.bytes());

                // Get
                Result result = table.get(get);

                // Extract files
                for (Cell cell : result.rawCells()) {
                    byte[] qualifier = CellUtil.cloneQualifier(cell);
                    if (cell.getValueLength() == 0) {
                        continue;
                    }
                    if (AbstractPhoenixConverter.endsWith(qualifier, VariantPhoenixHelper.FILE_SUFIX_BYTES)) {
                        String columnName = Bytes.toString(qualifier);
                        String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
//                            Integer studyId = Integer.valueOf(split[0]);
                        String fileId = split[1];
                        PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(
                                cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        filesMap.add(Pair.of(fileId, array));
                    }
                }

                // Extract stats
                Map<Integer, VariantStats> statsMap = statsConverter.convert(result).get(studyId);
                if (statsMap != null) {
                    for (Map.Entry<Integer, VariantStats> entry : statsMap.entrySet()) {
                        stats.put(metadataManager.getCohortName(studyId, entry.getKey()), entry.getValue());
                    }
                }
            });

            // Convert to VariantSampleData
            HBaseToStudyEntryConverter converter = new HBaseToStudyEntryConverter(
                    GenomeHelper.COLUMN_FAMILY_BYTES, metadataManager,
                    statsConverter);
            converter.setSelectVariantElements(
                    new VariantQueryFields(metadataManager.getStudyMetadata(studyId), samples, new ArrayList<>(fileIdsFromSampleIds)));

            StudyEntry studyEntry = converter.convert(sampleDataMap, filesMap, variant, studyId);

            Map<Integer, String> fileNameFromSampleId = new HashMap<>();
            for (Integer fileId : fileIdsFromSampleIds) {
                FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, fileId);
                for (Integer sample : fileMetadata.getSamples()) {
                    fileNameFromSampleId.put(sample, fileMetadata.getName());
                }
            }

            Map<String, List<SampleData>> sampleData = new HashMap<>();
            gtGroups.keySet().forEach(gt -> sampleData.put(gt, new ArrayList<>(limit)));
            for (String sampleName : studyEntry.getOrderedSamplesName()) {
                Map<String, String> map = studyEntry.getSampleDataAsMap(sampleName);
                String gt = map.get("GT");
                String groupGt;
                if (StringUtils.isEmpty(gt) || gt.equals(".")) {
                    groupGt = GenotypeClass.NA_GT_VALUE;
                } else {
                    groupGt = gtGroupsInverse.get(gt);
                }
                String fileId = fileNameFromSampleId.get(metadataManager.getSampleId(studyId, sampleName));
                sampleData.get(groupGt).add(new SampleData(sampleName, map, fileId));
            }
            Map<String, FileEntry> files = studyEntry.getFiles().stream().collect(Collectors.toMap(FileEntry::getFileId, f -> f));

            VariantSampleData variantSampleData = new VariantSampleData(variantStr, study, sampleData, files, stats);
            return new DataResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), 1,
                    Collections.singletonList(variantSampleData), 1);

        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected Map<String, Collection<String>> getGenotypeGroups(int studyId, Set<String> genotypes, boolean merge) {
        Map<String, Collection<String>> gtGroups = new LinkedHashMap<>(genotypes.size());
        List<String> loadedGts = metadataManager.getStudyMetadata(studyId).getAttributes()
                .getAsStringList(VariantStorageEngine.Options.LOADED_GENOTYPES.key());
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
