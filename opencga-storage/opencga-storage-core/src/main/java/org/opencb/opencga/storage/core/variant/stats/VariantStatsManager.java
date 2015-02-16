package org.opencb.opencga.storage.core.variant.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jmmut on 12/02/15.
 */
public class VariantStatsManager {

    private static final String BATCH_SIZE = "batchSize";
    private String VARIANT_STATS_SUFFIX = ".variants.stats.json.gz";
    private String SOURCE_STATS_SUFFIX = ".source.stats.json.gz";
    private final JsonFactory jsonFactory;
    private ObjectMapper jsonObjectMapper;
    protected static Logger logger = LoggerFactory.getLogger(VariantStatisticsCalculator.class);

    public VariantStatsManager() {
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
    }

    /**
     * retrieves batches of Variants, delegates to obtain VariantStatsWrappers from those Variants, and writes them to the output URI.
     * @param variantDBAdaptor to obtain the Variants
     * @param output where to write the VariantStats
     * @param options filters to the query, batch size, number of threads to use...
     *
     * @return outputUri prefix for the filename (without the "._type_.stats.json.gz")
     * @throws IOException
     */
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Set<String> samples, QueryOptions options) throws IOException {

        /** Open output streams **/
        Path fileVariantsPath = Paths.get(output.getPath() + VARIANT_STATS_SUFFIX);
        OutputStream outputVariantsStream = getOutputStream(fileVariantsPath, options);

        Path fileSourcePath = Paths.get(output.getPath() + SOURCE_STATS_SUFFIX);
        OutputStream outputSourceStream = getOutputStream(fileSourcePath, options);

        /** Initialize Json serializer **/
        ObjectWriter variantsWriter = jsonObjectMapper.writerWithType(VariantStatsWrapper.class);
        ObjectWriter sourceWriter = jsonObjectMapper.writerWithType(VariantSourceStats.class);


        /** Variables for statistics **/
        int batchSize = 100;  // future optimization, threads, etc
        if(options != null) { //Parse query options
            batchSize = options.getInt(BATCH_SIZE, batchSize);
        }
        List<Variant> variantBatch = new ArrayList<>(batchSize);
        VariantSource variantSource = options.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);   // TODO Is this retrievable from the adaptor?
        VariantSourceStats variantSourceStats = new VariantSourceStats(variantSource.getFileId(), variantSource.getStudyId());
        VariantStatisticsCalculator variantStatisticsCalculator = new VariantStatisticsCalculator();


        logger.info("starting stats calculation");
        long start = System.currentTimeMillis();

        Iterator<Variant> iterator = obtainIterator(variantDBAdaptor, options);
        while(iterator.hasNext()) {
            Variant variant = iterator.next();
            variantBatch.add(filterSample(variant, samples));

            if (variantBatch.size() == batchSize) {
                List<VariantStatsWrapper> variantStatsWrappers = variantStatisticsCalculator.calculateBatch(variantBatch, variantSource);

                for (VariantStatsWrapper variantStatsWrapper : variantStatsWrappers) {
                    outputVariantsStream.write(variantsWriter.writeValueAsBytes(variantStatsWrapper));
                }

                variantSourceStats.updateFileStats(variantBatch);
                variantSourceStats.updateSampleStats(variantBatch, variantSource.getPedigree());  // TODO test
            }
        }
        if (variantBatch.size() != 0) {
            List<VariantStatsWrapper> variantStatsWrappers = variantStatisticsCalculator.calculateBatch(variantBatch, variantSource);

            for (VariantStatsWrapper variantStatsWrapper : variantStatsWrappers) {
                outputVariantsStream.write(variantsWriter.writeValueAsBytes(variantStatsWrapper));
            }

            variantSourceStats.updateFileStats(variantBatch);
            variantSourceStats.updateSampleStats(variantBatch, variantSource.getPedigree());  // TODO test
        }
        logger.info("finishing stats calculation, time: {}ms", System.currentTimeMillis() - start);

        outputSourceStream.write(sourceWriter.writeValueAsString(variantSourceStats).getBytes());
        outputVariantsStream.close();
        outputSourceStream.close();
        return fileVariantsPath.toUri();
    }

    private OutputStream getOutputStream(Path filePath, QueryOptions options) throws IOException {
        OutputStream outputStream = new FileOutputStream(filePath.toFile());
        logger.info("will write stats to {}", filePath);
        if(options != null && options.getBoolean("gzip", true)) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }

    /** Gets iterator from OpenCGA Variant database. **/
    private Iterator<Variant> obtainIterator(VariantDBAdaptor variantDBAdaptor, QueryOptions options) {

        QueryOptions iteratorQueryOptions = new QueryOptions();
        if(options != null) { //Parse query options
            iteratorQueryOptions = options;
        }

        // TODO rethink this way to refer to the Variant fields (through DBObjectToVariantConverter)
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "sourceEntries");
        iteratorQueryOptions.add("include", include);

        return variantDBAdaptor.iterator(iteratorQueryOptions);
    }
    /**
     * Removes samples not present in 'samplesToKeep'
     * @param variant will be modified
     * @param samplesToKeep set of names of samples
     * @return variant with just the samples in 'samplesToKeep'
     */
    public Variant filterSample(Variant variant, Set<String> samplesToKeep) {
        List<String> toRemove = new ArrayList<>();
        if (samplesToKeep != null) {
            Collection<VariantSourceEntry> sourceEntries = variant.getSourceEntries().values();
            Map<String, VariantSourceEntry> filteredSourceEntries = new HashMap<>(sourceEntries.size());
            for (VariantSourceEntry sourceEntry : sourceEntries) {
                for (String name : sourceEntry.getSampleNames()) {
                    if (!samplesToKeep.contains(name)) {
                        toRemove.add(name);
                    }
                }
                Set<String> sampleNames = sourceEntry.getSampleNames();
                for (String name : toRemove) {
                    sampleNames.remove(name);
                }
            }
        }
        return variant;
    }

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {

        logger.info("starting stats loading from {}", uri);
        long start = System.currentTimeMillis();

        loadVariantStats(variantDBAdaptor, uri, options);
//        System.out.println("uri = " + uri);
        URI sourceUri = uri.resolve(uri.getPath().replace(VARIANT_STATS_SUFFIX, SOURCE_STATS_SUFFIX));
//        System.out.println("uri source = " + sourceUri);
        loadSourceStats(variantDBAdaptor, sourceUri, options);

        logger.info("finishing stats loading, time: {}ms", System.currentTimeMillis() - start);
    }
    public void loadVariantStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {

        /** Open input streams **/
        Path variantInput = Paths.get(uri.getPath());
        InputStream variantInputStream;
        variantInputStream = new FileInputStream(variantInput.toFile());
        variantInputStream = new GZIPInputStream(variantInputStream);

        /** Initialize Json parse **/
        JsonParser parser = jsonFactory.createParser(variantInputStream);

        int batchSize = options.getInt(BATCH_SIZE, 1000);
        ArrayList<VariantStatsWrapper> statsBatch = new ArrayList<>(batchSize);
        int writes = 0;
        int variantsNumber = 0;

        while (parser.nextToken() != null) {
            variantsNumber++;
            statsBatch.add(parser.readValueAs(VariantStatsWrapper.class));

            if (statsBatch.size() == batchSize) {
                QueryResult writeResult = variantDBAdaptor.updateStats(statsBatch, options);
                writes += writeResult.getNumResults();
                logger.info("stats loaded up to position {}", statsBatch.get(statsBatch.size()-1).getPosition());
                statsBatch.clear();
            }
        }

        if (!statsBatch.isEmpty()) {
            QueryResult writeResult = variantDBAdaptor.updateStats(statsBatch, options);
            writes += writeResult.getNumResults();
            logger.info("stats loaded up to position {}", statsBatch.get(statsBatch.size()-1).getPosition());
            statsBatch.clear();
        }

        if (writes < variantsNumber) {
            logger.warn("provided statistics of {} variants, but only {} were updated", variantsNumber, writes);
            logger.info("note: maybe those variants didn't had the proper study? maybe the new and the old stats were the same?");
        }

    }
    public void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {

        /** Open input streams **/
        Path sourceInput = Paths.get(uri.getPath());
        InputStream sourceInputStream;
        sourceInputStream = new FileInputStream(sourceInput.toFile());
        sourceInputStream = new GZIPInputStream(sourceInputStream);

        /** Initialize Json parse **/
        JsonParser sourceParser = jsonFactory.createParser(sourceInputStream);

        VariantSourceStats variantSourceStats;
//        if (sourceParser.nextToken() != null) {
        variantSourceStats = sourceParser.readValueAs(VariantSourceStats.class);
//        }
        VariantSource variantSource = options.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);   // needed?

        // TODO if variantSourceStats doesn't have studyId and fileId, create another with variantSource.getStudyId() and variantSource.getFileId()
        variantDBAdaptor.getVariantSourceDBAdaptor().updateSourceStats(variantSourceStats, options);

//        DBObject studyMongo = sourceConverter.convertToStorageType(source);
//        DBObject query = new BasicDBObject(DBObjectToVariantSourceConverter.FILEID_FIELD, source.getFileName());
//        WriteResult wr = filesCollection.update(query, studyMongo, true, false);
    }

}
