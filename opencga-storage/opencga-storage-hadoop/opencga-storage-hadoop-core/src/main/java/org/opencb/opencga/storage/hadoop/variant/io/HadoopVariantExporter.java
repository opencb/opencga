package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;

/**
 * Created on 11/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantExporter extends VariantExporter {

    private final MRExecutor mrExecutor;

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
            StudyConfiguration defaultStudyConfiguration =
                    VariantQueryUtils.getDefaultStudyConfiguration(query, queryOptions, dbAdaptor.getStudyConfigurationManager());
            // TODO: Should accept multi-study export!
            int studyId = defaultStudyConfiguration.getStudyId();

            ObjectMap options = new ObjectMap(engine.getOptions())
                    .append("--output", outputFileUri.toString())
                    .append("--of", outputFormat.toString());
            options.putAll(query);
            options.putAll(queryOptions);

            String args = VariantExporterDriver.buildCommandLineArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    Collections.emptyList(),
                    options
            );

            mrExecutor.run(VariantExporterDriver.class, args, engine.getOptions(), "Export variants");

            // TODO: Write metadata!
//            VariantMetadata metadata = metadataFactory.makeVariantMetadata(query, queryOptions);
//            writeMetadata(metadata, outputFile + METADATA_FILE_EXTENSION);

        } else {
            throw new IllegalArgumentException("Unknown output scheme '" + outputFileUri.getScheme() + "' for file " + outputFileUri);
        }

    }
}
