package org.opencb.opencga.storage.hadoop.variant.adaptors.sample;

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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.sample.SampleData;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleData;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleDataManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    public QueryResult<VariantSampleData> getSampleData(String variantStr, String study, QueryOptions options,
                                                        List<String> includeSamples,
                                                        Set<String> genotypes,
                                                        boolean merge, int sampleLimit) {
        if (StringUtils.isNotEmpty(options.getString(VariantQueryParam.INCLUDE_SAMPLE.key()))
                && !VariantQueryUtils.ALL.equals(options.getString(VariantQueryParam.INCLUDE_SAMPLE.key()))) {
            super.getSampleData(variantStr, study, options);
        }
        StopWatch stopWatch = StopWatch.createStarted();

        int studyId = metadataManager.getStudyId(study);

        Map<String, Collection<String>> gtGroups = getGenotypeGroups(studyId, genotypes, merge);
        Map<String, String> gtGroupsInverse = new HashMap<>();
        gtGroups.forEach((gt, subGts) -> subGts.forEach(subGt -> gtGroupsInverse.put(subGt, gt)));


        int skip = Math.max(0, options.getInt(QueryOptions.SKIP, 0));
        int limit = Math.max(0, options.getInt(QueryOptions.LIMIT, 10));

        Variant variant = new Variant(variantStr);
        try {
            List<Get> sampleGets = new ArrayList<>(gtGroups.size());
            for (Map.Entry<String, Collection<String>> entry : gtGroups.entrySet()) {

                Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
                LinkedList<Filter> filters = new LinkedList<>();
                LinkedList<Filter> genotypeFilters = new LinkedList<>();

                filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryPrefixComparator(Bytes.toBytes(buildStudyColumnsPrefix(studyId)))));

                for (String genotype : entry.getValue()) {
                    byte[] gtBytes = new byte[genotype.length() + 1];
                    System.arraycopy(Bytes.toBytes(genotype), 0, gtBytes, 0, genotype.length());

                    genotypeFilters.add(new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(gtBytes)));
                }

                filters.add(new FilterList(FilterList.Operator.MUST_PASS_ONE, genotypeFilters));
                filters.add(new ColumnPaginationFilter(limit, skip));
                get.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters));
                sampleGets.add(get);
            }

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
                        if (AbstractPhoenixConverter.endsWith(qualifier, VariantPhoenixHelper.SAMPLE_DATA_SUFIX_BYTES)) {
                            String columnName = Bytes.toString(qualifier);
                            String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
//                            Integer studyId = Integer.valueOf(split[0]);
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


            List<Pair<String, PhoenixArray>> filesMap = new ArrayList<>();
            Set<Integer> fileIdsFromSampleIds = metadataManager.getFileIdsFromSampleIds(studyId, samples);
//            Integer cohortId = metadataManager.getCohortId(studyId, StudyEntry.DEFAULT_COHORT);
            dbAdaptor.getHBaseManager().act(dbAdaptor.getVariantTable(), table -> {
                Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
                for (Integer fileId : fileIdsFromSampleIds) {
                    get.addColumn(dbAdaptor.getGenomeHelper().getColumnFamily(), VariantPhoenixHelper.buildFileColumnKey(studyId, fileId));
                }
                Result result = table.get(get);
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
            });

            HBaseToStudyEntryConverter converter = new HBaseToStudyEntryConverter(
                    dbAdaptor.getGenomeHelper().getColumnFamily(), metadataManager,
                    new HBaseToVariantStatsConverter(dbAdaptor.getGenomeHelper()));
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
            for (String sampleName : studyEntry.getOrderedSamplesName()) {
                Map<String, String> map = studyEntry.getSampleDataAsMap(sampleName);
                String gt = gtGroupsInverse.get(map.get("GT"));
                // TODO: Support somatic variants
                if (gt == null) {
                    continue;
                }
                String fileId = fileNameFromSampleId.get(metadataManager.getSampleId(studyId, sampleName));
                sampleData.computeIfAbsent(gt, k -> new ArrayList<>(limit)).add(new SampleData(sampleName, map, fileId));
            }
            Map<String, FileEntry> files = studyEntry.getFiles().stream().collect(Collectors.toMap(FileEntry::getFileId, f -> f));

            VariantSampleData variantSampleData = new VariantSampleData(variantStr, study, sampleData, files);
            return new QueryResult<>("", (int) stopWatch.getTime(TimeUnit.MILLISECONDS), 1, 1, null, null,
                    Collections.singletonList(variantSampleData));

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
                Genotype genotype = new Genotype(genotypeStr);
                allGts.addAll(GenotypeClass.getPhasedGenotypes(genotype, loadedGts));
            }
            gtGroups.put(VariantQueryUtils.ALL, allGts);
        } else {
            for (String genotypeStr : genotypes) {
                List<String> allGts = new ArrayList<>(3);
                allGts.add(genotypeStr);
                Genotype genotype = new Genotype(genotypeStr);
                allGts.addAll(GenotypeClass.getPhasedGenotypes(genotype, loadedGts));
                gtGroups.put(genotypeStr, allGts);
            }
        }
        return gtGroups;
    }
}
