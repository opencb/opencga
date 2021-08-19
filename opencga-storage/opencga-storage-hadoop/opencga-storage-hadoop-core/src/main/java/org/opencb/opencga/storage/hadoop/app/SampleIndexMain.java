package org.opencb.opencga.storage.hadoop.app;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.io.bit.BitInputStream;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleVariantIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class SampleIndexMain extends AbstractMain {

    public static void main(String[] args) {
        SampleIndexMain main = new SampleIndexMain();
        try {
            main.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        String command = safeArg(args, 0, "help");
        String dbName = safeArg(args, 1);

        final HBaseManager hBaseManager;
        SampleIndexDBAdaptor dbAdaptor = null;
        VariantStorageMetadataManager metadataManager = null;
        if (command.equals("help")) {
            hBaseManager = null;
        } else {
            hBaseManager = new HBaseManager(configuration);
            hBaseManager.getConnection();
            HBaseVariantTableNameGenerator tableNameGenerator = new HBaseVariantTableNameGenerator(dbName, configuration);
            metadataManager = new VariantStorageMetadataManager(
                    new HBaseVariantStorageMetadataDBAdaptorFactory(hBaseManager, tableNameGenerator.getMetaTableName(), configuration));
            dbAdaptor = new SampleIndexDBAdaptor(
                    hBaseManager, tableNameGenerator,
                    metadataManager);
            if (!hBaseManager.tableExists(tableNameGenerator.getMetaTableName())) {
                throw new IllegalArgumentException("Metadata table '" + tableNameGenerator.getMetaTableName() + "' does not exist");
            }
        }

        ObjectMap argsMap = getArgsMap(args, 2);
        if (argsMap.getBoolean("help", false)) {
            command = "help";
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        SampleIndexQuery sampleIndexQuery;
        switch (command) {
            case "count":
                sampleIndexQuery = dbAdaptor.parseSampleIndexQuery(new Query(argsMap));
                print(dbAdaptor.count(sampleIndexQuery));
                break;
            case "query": {
                query(dbAdaptor, argsMap);
                break;
            }
            case "query-detailed": {
                detailedQuery(dbAdaptor, argsMap);
                break;
            }
            case "query-raw": {
                rawQuery(dbAdaptor, argsMap);
                break;
            }
            case "index-stats": {
                indexStats(dbAdaptor, argsMap);
                break;
            }
            case "help":
            default:
                System.out.println("Commands:");
                System.out.println("  help");
                System.out.println("  count           <databaseName> [--study <study>] [--sample <sample>] [--region <region>] ...");
                System.out.println("  query           <databaseName> [--study <study>] [--sample <sample>] [--region <region>] [--quiet] "
                        + "[query...]");
                System.out.println("  query-detailed  <databaseName> [--study <study>] [--sample <sample>] [--region <region>] [--quiet] "
                        + "[query...]");
                System.out.println("  query-raw       <databaseName> [--study <study>] [--sample <sample>] [--quiet]");
                System.out.println("  index-stats     <databaseName> [--study <study>] [--sample <sample>]");
                break;
        }
        System.err.println("--------------------------");
        System.err.println("  Wall time: " + TimeUtils.durationToString(stopWatch));
        System.err.println("--------------------------");
        if (hBaseManager != null) {
            hBaseManager.close();
        }
    }

    private void query(SampleIndexDBAdaptor dbAdaptor, ObjectMap argsMap) throws Exception {
        SampleIndexQuery sampleIndexQuery = dbAdaptor.parseSampleIndexQuery(new Query(argsMap));
        VariantDBIterator iterator = dbAdaptor.iterator(sampleIndexQuery);
        if (argsMap.getBoolean("quiet", false)) {
            print(Iterators.size(iterator));
            iterator.close();
        } else {
            print(iterator);
        }
        System.err.println("Time Fetching   : " + TimeUtils.durationToString(iterator.getTimeFetching(TimeUnit.MILLISECONDS)));
        System.err.println("Time Converting : " + TimeUtils.durationToString(iterator.getTimeConverting(TimeUnit.MILLISECONDS)));
    }

    private void rawQuery(SampleIndexDBAdaptor dbAdaptor, ObjectMap argsMap) throws Exception {
        SampleIndexQuery sampleIndexQuery = dbAdaptor.parseSampleIndexQuery(new Query(argsMap));
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        int studyId = metadataManager.getStudyId(sampleIndexQuery.getStudy());
        if (sampleIndexQuery.getSamplesMap().size() != 1) {
            printError("Unable to get raw content from more than one sample at a time");
            return;
        }

        int sampleId = metadataManager.getSampleIdOrFail(studyId, sampleIndexQuery.getSamplesMap().keySet().iterator().next());
        Region region = argsMap.containsKey(VariantQueryParam.REGION.key())
                ? Region.parseRegion(argsMap.getString(VariantQueryParam.REGION.key()))
                : null;
        CloseableIterator<SampleIndexEntry> iterator = dbAdaptor.rawIterator(studyId, sampleId, region);
        if (argsMap.getBoolean("quiet", false)) {
            print(Iterators.size(iterator));
        } else {
            iterator.forEachRemaining(v -> {
                System.out.println(" ----------------------- ");
                System.out.println(v.toString());
            });
        }
        iterator.close();
    }

    private void detailedQuery(SampleIndexDBAdaptor dbAdaptor, ObjectMap argsMap) throws Exception {
        SampleIndexQuery sampleIndexQuery = dbAdaptor.parseSampleIndexQuery(new Query(argsMap));
        CloseableIterator<SampleVariantIndexEntry> iterator = dbAdaptor.rawIterator(sampleIndexQuery);
        SampleIndexSchema schema = dbAdaptor.getSchema(sampleIndexQuery.getStudy());
        if (argsMap.getBoolean("quiet", false)) {
            print(Iterators.size(iterator));
        } else {
            iterator.forEachRemaining(v -> {
                System.out.println(" ----------------------- ");
                System.out.println(v.toString(schema));
            });
        }
        iterator.close();
    }

    private void indexStats(SampleIndexDBAdaptor dbAdaptor, ObjectMap argsMap) throws Exception {



        SampleIndexQuery sampleIndexQuery = dbAdaptor.parseSampleIndexQuery(new Query(argsMap));
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        int studyId = metadataManager.getStudyId(sampleIndexQuery.getStudy());
        if (sampleIndexQuery.getSamplesMap().size() != 1) {
            printError("Unable to get raw content from more than one sample at a time");
            return;
        }

        int sampleId = metadataManager.getSampleIdOrFail(studyId, sampleIndexQuery.getSamplesMap().keySet().iterator().next());
        Region region = argsMap.containsKey(VariantQueryParam.REGION.key())
                ? Region.parseRegion(argsMap.getString(VariantQueryParam.REGION.key()))
                : null;

        Map<String, Integer> counts = new TreeMap<>();
        try (CloseableIterator<SampleIndexEntry> iterator = dbAdaptor.rawIterator(studyId, sampleId, region)) {
            while (iterator.hasNext()) {
                SampleIndexEntry entry = iterator.next();
                for (SampleIndexEntry.SampleIndexGtEntry gtEntry : entry.getGts().values()) {
                    String gt = gtEntry.getGt();

                    counts.merge("Variants_bytes", gtEntry.getVariantsLength(), Integer::sum);
                    addLength(gt, counts, "FileIndex", gtEntry.getFileIndexStream());
                    addLength(gt, counts, "PopulationFrequencyIndex", gtEntry.getPopulationFrequencyIndexStream());
                    addLength(gt, counts, "CtBtTfIndex", gtEntry.getCtBtTfIndexStream());
                    addLength(gt, counts, "biotypeIndex", gtEntry.getBiotypeIndexStream());
                    addLength(gt, counts, "ctIndex", gtEntry.getConsequenceTypeIndexStream());
                    addLength(gt, counts, "clinicalIndex", gtEntry.getClinicalIndexStream());
                    addLength(gt, counts, "transcriptFlagIndex", gtEntry.getTranscriptFlagIndexStream());
                }
            }
        }
        print(counts);
    }

    private void addLength(String gt, Map<String, Integer> counts, String key, BitInputStream stream) {
        if (stream != null) {
            counts.merge(key + "_bytes", stream.getByteLength(), Integer::sum);
            counts.merge(key + "_count", 1, Integer::sum);
            counts.merge(gt + "_" + key + "_bytes", stream.getByteLength(), Integer::sum);
            counts.merge(gt + "_" + key + "_count", 1, Integer::sum);
        }
    }

}
