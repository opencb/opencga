/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.app.cli;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 25/05/15.
 */
public class AnnotateVariantsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.AnnotateVariantsCommandOptions annotateVariantsCommandOptions;


    public AnnotateVariantsCommandExecutor(CliOptionsParser.AnnotateVariantsCommandOptions annotateVariantsCommandOptions) {
        super(annotateVariantsCommandOptions.logLevel, annotateVariantsCommandOptions.verbose,
                annotateVariantsCommandOptions.configFile);

        this.logFile = annotateVariantsCommandOptions.logFile;
        this.annotateVariantsCommandOptions = annotateVariantsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        /**
         * Create DBAdaptor
         */
        VariantStorageManager variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(annotateVariantsCommandOptions.storageEngine);

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(annotateVariantsCommandOptions.dbName);

        /**
         * Create Annotator
         */

        ObjectMap options = configuration.getStorageEngine(annotateVariantsCommandOptions.storageEngine).getVariant().getOptions();
        if (annotateVariantsCommandOptions.annotator != null) options.put(VariantAnnotationManager.ANNOTATION_SOURCE, annotateVariantsCommandOptions.annotator);
        if (annotateVariantsCommandOptions.species != null) options.put(VariantAnnotationManager.SPECIES, annotateVariantsCommandOptions.species);
        if (annotateVariantsCommandOptions.assembly != null) options.put(VariantAnnotationManager.ASSEMBLY, annotateVariantsCommandOptions.assembly);

        VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(configuration, annotateVariantsCommandOptions.storageEngine);
//            VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(annotatorSource, annotatorProperties, annotateVariantsCommandOptions.species, annotateVariantsCommandOptions.assembly);
        VariantAnnotationManager variantAnnotationManager = new VariantAnnotationManager(annotator, dbAdaptor);

        /**
         * Annotation options
         */
        Query query = new Query();
        if (annotateVariantsCommandOptions.filterRegion != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), annotateVariantsCommandOptions.filterRegion);
        }
        if (annotateVariantsCommandOptions.filterChromosome != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), annotateVariantsCommandOptions.filterChromosome);
        }
        if (annotateVariantsCommandOptions.filterGene != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), annotateVariantsCommandOptions.filterGene);
        }
        if (annotateVariantsCommandOptions.filterAnnotConsequenceType != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), annotateVariantsCommandOptions.filterAnnotConsequenceType);
        }
        if (!annotateVariantsCommandOptions.overwriteAnnotations) {
            query.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), false);
        }
        URI outputUri = UriUtils.createUri(annotateVariantsCommandOptions.outdir == null ? "." : annotateVariantsCommandOptions.outdir);
        Path outDir = Paths.get(outputUri.resolve(".").getPath());

        /**
         * Create and load annotations
         */
        boolean doCreate = annotateVariantsCommandOptions.create, doLoad = annotateVariantsCommandOptions.load != null;
        if (!annotateVariantsCommandOptions.create && annotateVariantsCommandOptions.load == null) {
            doCreate = true;
            doLoad = true;
        }

        URI annotationFile = null;
        if (doCreate) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation creation ");
            annotationFile = variantAnnotationManager.createAnnotation(outDir,
                    annotateVariantsCommandOptions.fileName == null ? annotateVariantsCommandOptions.dbName : annotateVariantsCommandOptions.fileName,
                    query, new QueryOptions());
            logger.info("Finished annotation creation {}ms", System.currentTimeMillis() - start);
        }

        if (doLoad) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation load");
            if (annotationFile == null) {
//                annotationFile = new URI(null, c.load, null);
                annotationFile = Paths.get(annotateVariantsCommandOptions.load).toUri();
            }
            variantAnnotationManager.loadAnnotation(annotationFile, new QueryOptions());
            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
        }
    }
}
