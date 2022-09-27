package org.opencb.opencga.storage.hadoop.variant.io;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.io.managers.IOConnector;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.io.managers.LocalIOConnector;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.BreakendVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.query.executors.DBAdaptorVariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.search.SearchIndexVariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexCompoundHeterozygousQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexMendelianErrorQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexOnlyVariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.SampleIndexVariantQueryExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.getSearchEngineQuery;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.*;

/**
 * Created on 11/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantExporter extends VariantExporter {

    private final HadoopVariantStorageEngine engine;
    private final MRExecutor mrExecutor;
    private final Logger logger = LoggerFactory.getLogger(HadoopVariantExporter.class);

    public HadoopVariantExporter(HadoopVariantStorageEngine engine, VariantMetadataFactory metadataFactory, MRExecutor mrExecutor,
                                 IOConnectorProvider ioConnectorProvider)
            throws StorageEngineException {
        super(engine, metadataFactory, ioConnectorProvider);
        this.engine = engine;
        this.mrExecutor = mrExecutor;
    }

    @Override
    public List<URI> export(@Nullable URI outputFileUri, VariantWriterFactory.VariantOutputFormat outputFormat, URI variantsFile,
                            ParsedVariantQuery variantQuery)
            throws IOException, StorageEngineException {
        VariantHadoopDBAdaptor dbAdaptor = ((VariantHadoopDBAdaptor) engine.getDBAdaptor());
        IOConnector ioConnector = ioConnectorProvider.get(outputFileUri);

        // Use pre-processed query instead of input query
        Query query = variantQuery.getQuery();
        QueryOptions queryOptions = variantQuery.getInputOptions();
        boolean smallQuery = false;
        if (!queryOptions.getBoolean("skipSmallQuery", false)) {
            ParsedVariantQuery.VariantQueryXref xrefs = VariantQueryParser.parseXrefs(query);
            if (xrefs.getVariants().size() > 0 && xrefs.getVariants().size() < 2000) {
                // FIXME: Is this scenario still needed?
                if (!VariantQueryUtils.isValidParam(query, VariantQueryParam.REGION)
                        && xrefs.getGenes().isEmpty()
                        && xrefs.getIds().isEmpty()
                        && xrefs.getOtherXrefs().isEmpty()) {
                    logger.info("Query for {} variants. Consider small query. Skip MapReduce", xrefs.getVariants().size());
                    smallQuery = true;
                }
            }

            VariantQueryExecutor queryExecutor = engine.getVariantQueryExecutor(query, queryOptions);
            if (queryExecutor instanceof SampleIndexCompoundHeterozygousQueryExecutor
                    || queryExecutor instanceof BreakendVariantQueryExecutor
                    || queryExecutor instanceof SampleIndexMendelianErrorQueryExecutor
                    || queryExecutor instanceof SampleIndexOnlyVariantQueryExecutor) {
                logger.info("Query using special VariantQueryExecutor {}. Skip MapReduce", queryExecutor.getClass());
                smallQuery = true;
            } else if (queryOptions.getInt(QueryOptions.LIMIT, -1) > 0 || queryOptions.getInt(QueryOptions.SKIP, -1) > 0) {
                logger.info("Do not use MapReduce when exporting a paginated result.");
                smallQuery = true;
            } else if (queryExecutor instanceof SampleIndexVariantQueryExecutor) {
                if (SampleIndexQueryParser.validSampleIndexQuery(query)) {
                    int samplesThreshold = engine.getOptions().getInt(
                            EXPORT_SMALL_QUERY_SAMPLE_INDEX_SAMPLES_THRESHOLD.key(),
                            EXPORT_SMALL_QUERY_SAMPLE_INDEX_SAMPLES_THRESHOLD.defaultValue());
                    if (variantQuery.getStudyQuery().countSamplesInFilter() < samplesThreshold) {
                        int variantsThreshold = engine.getOptions().getInt(
                                EXPORT_SMALL_QUERY_SAMPLE_INDEX_VARIANTS_THRESHOLD.key(),
                                EXPORT_SMALL_QUERY_SAMPLE_INDEX_VARIANTS_THRESHOLD.defaultValue());
                        try {
                            long numMatches = engine.get(new Query(query), new QueryOptions(queryOptions)
                                    .append(QueryOptions.LIMIT, 1)
                                    .append(QueryOptions.SKIP, 0)
                                    .append(QueryOptions.COUNT, true)
                            ).getNumMatches();

                            if (numMatches < variantsThreshold) {
                                logger.info("Query with {} samples matching {} variants. Consider small query. Skip MapReduce",
                                        variantQuery.getStudyQuery().countSamplesInFilter(), numMatches);
                                smallQuery = true;
                            } else {
                                logger.info("Query with {} samples matching {} variants. Current variants threshold is {}."
                                                + " Not a small query.",
                                        variantQuery.getStudyQuery().countSamplesInFilter(), numMatches, variantsThreshold);
                            }
                        } catch (Exception e) {
                            logger.info("Unable to count variants from SampleIndex", e);
                        }
                    }
                }
            } else if (queryExecutor instanceof DBAdaptorVariantQueryExecutor) {
                if (VariantHBaseQueryParser.isSupportedQuery(query)) {
                    int variantsThreshold = engine.getOptions().getInt(
                            EXPORT_SMALL_QUERY_SCAN_VARIANTS_THRESHOLD.key(),
                            EXPORT_SMALL_QUERY_SCAN_VARIANTS_THRESHOLD.defaultValue());
                    if (engine.secondaryAnnotationIndexActiveAndAlive()) {
                        try {
                            long totalCount = engine.getVariantSearchManager().count(engine.getDBName(), new Query());
                            long count = engine.getVariantSearchManager().count(engine.getDBName(), getSearchEngineQuery(query));
                            if (count < variantsThreshold) {
                                logger.info("Query for approximately {} of {} variants, using HBase native SCAN."
                                                + " Consider small query."
                                                + " Skip MapReduce",
                                        count, totalCount);
                                smallQuery = true;
                            } else {
                                logger.info("Query for approximately {} of {} variants, using HBase native SCAN."
                                                + " Current variants threshold is {}."
                                                + " Not a small query",
                                        count, totalCount, variantsThreshold);
                            }
                        } catch (VariantSearchException e) {
                            logger.info("Unable to count variants from SearchEngine", e);
                        }
                    }
                }
            } else if (queryExecutor instanceof SearchIndexVariantQueryExecutor) {
                // If the query can be resolved with the secondary annotation index (i.e. Solr), we can
                // check how small the query is and get an estimation. If it's less than a threshold, we can skip the mapreduce.
                int variantsThreshold = engine.getOptions().getInt(
                        EXPORT_SMALL_QUERY_SEARCH_INDEX_VARIANTS_THRESHOLD.key(),
                        EXPORT_SMALL_QUERY_SEARCH_INDEX_VARIANTS_THRESHOLD.defaultValue());
                float matchRatioThreshold = engine.getOptions().getFloat(
                        EXPORT_SMALL_QUERY_SEARCH_INDEX_MATCH_RATIO_THRESHOLD.key(),
                        EXPORT_SMALL_QUERY_SEARCH_INDEX_MATCH_RATIO_THRESHOLD.defaultValue());
                try {
                    long totalCount = engine.getVariantSearchManager().count(engine.getDBName(), new Query());
                    long count = engine.getVariantSearchManager().count(engine.getDBName(), getSearchEngineQuery(query));
                    double matchRate = ((double) count) / ((double) totalCount);
                    logger.info("Count {}/{} variants from query {}", count, totalCount, getSearchEngineQuery(query));
                    if (count < variantsThreshold || matchRate < matchRatioThreshold) {
                        logger.info("Query for approximately {} of {} variants, which is {}% of the total."
                                        + " Consider small query."
                                        + " Skip MapReduce",
                                count, totalCount, matchRate * 100);
                        smallQuery = true;
                    } else {
                        logger.info("Query for approximately {} of {} variants, which is {}% of the total."
                                        + " Current variants threshold is {}, and matchRatioThreshold is {}% ."
                                        + " Not a small query",
                                count, totalCount, matchRate * 100, variantsThreshold, matchRatioThreshold);
                    }
                } catch (VariantSearchException e) {
                    logger.info("Unable to count variants from SearchEngine", e);
                }
            }
        }

        if ((outputFileUri == null)
                || (variantsFile != null)
                || smallQuery
                || queryOptions.getBoolean("skipMapReduce", false)
                || (!(ioConnector instanceof HDFSIOConnector) && !(ioConnector instanceof LocalIOConnector))) {
            return super.export(outputFileUri, outputFormat, variantsFile, variantQuery);
        } else {
            outputFileUri = VariantWriterFactory.checkOutput(outputFileUri, outputFormat);
            Path outputPath = new Path(outputFileUri);
            FileSystem fileSystem = outputPath.getFileSystem(dbAdaptor.getConfiguration());
            if (fileSystem.exists(outputPath)) {
                throw new IOException("Output file " + outputFileUri + " already exists!");
            }

            String metaFilename = outputFileUri.toString() + METADATA_FILE_EXTENSION;
            if (outputFormat == VariantWriterFactory.VariantOutputFormat.TPED) {
                metaFilename = outputFileUri.toString().replace(TPED_FILE_EXTENSION, TFAM_FILE_EXTENSION);
            }

            Path metadataPath = new Path(metaFilename);
            if (fileSystem.exists(metadataPath)) {
                throw new IOException("Output file " + metadataPath + " already exists!");
            }

            ObjectMap options = new ObjectMap(engine.getOptions())
                    .append(VariantExporterDriver.OUTPUT_PARAM, outputFileUri.toString())
                    .append(VariantExporterDriver.OUTPUT_FORMAT_PARAM, outputFormat.toString());
            options.putAll(queryOptions);
            options.putAll(query);

            String[] args = VariantExporterDriver.buildArgs(dbAdaptor.getVariantTable(), options);

            mrExecutor.run(VariantExporterDriver.class, args, "Export variants");

            VariantMetadata metadata = metadataFactory.makeVariantMetadata(query, queryOptions);
            writeMetadata(metadata, metadataPath.toUri());
            //writeMetadataInHdfs(metadata, metadataPath, fileSystem);

            logger.info("Output file : " + outputPath);
            logger.info("Output metadata file : " + metadataPath);

            return Arrays.asList(outputFileUri, metadataPath.toUri());
        }
    }

    @Override
    protected DataWriter<Variant> newVariantDataWriter(URI outputFile, OutputStream outputStream,
                                                       VariantWriterFactory.VariantOutputFormat outputFormat,
                                                       Query query, QueryOptions queryOptions) throws IOException {
        if (outputFormat == VariantWriterFactory.VariantOutputFormat.PARQUET) {
            return new VariantParquetWriter(outputFile, CompressionCodecName.UNCOMPRESSED, engine.getConf());
        } else if (outputFormat == VariantWriterFactory.VariantOutputFormat.PARQUET_GZ) {
            return new VariantParquetWriter(outputFile, CompressionCodecName.GZIP, engine.getConf());
        } else {
            return super.newVariantDataWriter(outputFile, outputStream, outputFormat, query, queryOptions);
        }
    }

    protected void writeMetadataInHdfs(VariantMetadata metadata, Path metadataPath, FileSystem fileSystem) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(metadataPath);
             OutputStream os = new GZIPOutputStream(fsDataOutputStream)) {
            objectMapper.writeValue(os, metadata);
        }
    }
}
