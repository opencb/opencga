package org.opencb.opencga.storage.hadoop.variant.io;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
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

    public HadoopVariantExporter(HadoopVariantStorageEngine engine, VariantMetadataFactory metadataFactory, MRExecutor mrExecutor)
            throws StorageEngineException {
        super(engine, metadataFactory);
        this.mrExecutor = mrExecutor;
    }

    @Override
    public void export(@Nullable URI outputFileUri, VariantWriterFactory.VariantOutputFormat outputFormat, Query query,
                       QueryOptions queryOptions)
            throws IOException, StorageEngineException {
        if (outputFileUri == null || StringUtils.isEmpty(outputFileUri.getScheme()) || outputFileUri.getScheme().equals("file")) {
            super.export(outputFileUri, outputFormat, query, queryOptions);
        } else if (outputFileUri.getScheme().equals("hdfs")) {
            VariantHadoopDBAdaptor dbAdaptor = ((VariantHadoopDBAdaptor) engine.getDBAdaptor());
            FileSystem fileSystem = FileSystem.get(dbAdaptor.getConfiguration());
            Path outputPath = new Path(outputFileUri);
            if (fileSystem.exists(outputPath)) {
                throw new IOException("Output directory " + outputFileUri + " already exists!");
            }
            Path metadataPath = new Path(outputFileUri.toString() + METADATA_FILE_EXTENSION);
            if (fileSystem.exists(metadataPath)) {
                throw new IOException("Output file " + outputFileUri + " already exists!");
            }

            ObjectMap options = new ObjectMap(engine.getOptions())
                    .append(VariantExporterDriver.OUTPUT_PARAM, outputFileUri.toString())
                    .append(VariantExporterDriver.OUTPUT_FORMAT_PARAM, outputFormat.toString());
            options.putAll(query);
            options.putAll(queryOptions);

            String[] args = VariantExporterDriver.buildArgs(dbAdaptor.getVariantTable(), options);

            mrExecutor.run(VariantExporterDriver.class, args, engine.getOptions(), "Export variants");

            VariantMetadata metadata = metadataFactory.makeVariantMetadata(query, queryOptions);
            writeMetadataInHdfs(metadata, metadataPath, fileSystem);

            logger.info("Output file : " + outputPath.toString());
            logger.info("Output metadata file : " + metadataPath.toString());
        } else {
            throw new IllegalArgumentException("Unknown output scheme '" + outputFileUri.getScheme() + "' for file " + outputFileUri);
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
