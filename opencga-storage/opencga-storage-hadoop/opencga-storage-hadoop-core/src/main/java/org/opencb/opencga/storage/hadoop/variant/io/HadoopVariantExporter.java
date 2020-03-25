package org.opencb.opencga.storage.hadoop.variant.io;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnector;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.io.managers.LocalIOConnector;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.zip.GZIPOutputStream;

/**
 * Created on 11/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantExporter extends VariantExporter {

    private final MRExecutor mrExecutor;
    private final Logger logger = LoggerFactory.getLogger(HadoopVariantExporter.class);

    public HadoopVariantExporter(HadoopVariantStorageEngine engine, VariantMetadataFactory metadataFactory, MRExecutor mrExecutor,
                                 IOConnectorProvider ioConnectorProvider)
            throws StorageEngineException {
        super(engine, metadataFactory, ioConnectorProvider);
        this.mrExecutor = mrExecutor;
    }

    @Override
    public void export(@Nullable URI outputFileUri, VariantWriterFactory.VariantOutputFormat outputFormat, URI variantsFile, Query query,
                       QueryOptions queryOptions)
            throws IOException, StorageEngineException {
        VariantHadoopDBAdaptor dbAdaptor = ((VariantHadoopDBAdaptor) engine.getDBAdaptor());
        IOConnector ioConnector = ioConnectorProvider.get(outputFileUri);

        boolean smallQuery = false;
        ParsedVariantQuery.VariantQueryXref xrefs = VariantQueryParser.parseXrefs(query);
        if (xrefs.getVariants().size() < 2000) {
            if (!VariantQueryUtils.isValidParam(query, VariantQueryParam.REGION)
                    && xrefs.getGenes().isEmpty()
                    && xrefs.getIds().isEmpty()
                    && xrefs.getOtherXrefs().isEmpty()) {
                smallQuery = true;
            }
        }

        if ((outputFileUri == null)
                || (variantsFile != null)
                || smallQuery
                || queryOptions.getBoolean("skipMapReduce", false)
                || (!(ioConnector instanceof HDFSIOConnector) && !(ioConnector instanceof LocalIOConnector))) {
            super.export(outputFileUri, outputFormat, variantsFile, query, queryOptions);
        } else {
            Path outputPath = new Path(outputFileUri);
            FileSystem fileSystem = outputPath.getFileSystem(dbAdaptor.getConfiguration());
            if (fileSystem.exists(outputPath)) {
                throw new IOException("Output directory " + outputFileUri + " already exists!");
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

            mrExecutor.run(VariantExporterDriver.class, args, engine.getOptions(), "Export variants");

            VariantMetadata metadata = metadataFactory.makeVariantMetadata(query, queryOptions);
            writeMetadata(metadata, metadataPath.toUri());
            //writeMetadataInHdfs(metadata, metadataPath, fileSystem);

            logger.info("Output file : " + outputPath.toString());
            logger.info("Output metadata file : " + metadataPath.toString());
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
