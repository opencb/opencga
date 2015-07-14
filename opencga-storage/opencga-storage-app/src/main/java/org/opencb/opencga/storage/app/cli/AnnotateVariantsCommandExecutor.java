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
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
        QueryOptions queryOptions = new QueryOptions();
        if (annotateVariantsCommandOptions.filterRegion != null) {
            queryOptions.add(VariantDBAdaptor.REGION, annotateVariantsCommandOptions.filterRegion);
        }
        if (annotateVariantsCommandOptions.filterChromosome != null) {
            queryOptions.add(VariantDBAdaptor.CHROMOSOME, annotateVariantsCommandOptions.filterChromosome);
        }
        if (annotateVariantsCommandOptions.filterGene != null) {
            queryOptions.add(VariantDBAdaptor.GENE, annotateVariantsCommandOptions.filterGene);
        }
        if (annotateVariantsCommandOptions.filterAnnotConsequenceType != null) {
            queryOptions.add(VariantDBAdaptor.ANNOT_CONSEQUENCE_TYPE, annotateVariantsCommandOptions.filterAnnotConsequenceType);
        }
        if (!annotateVariantsCommandOptions.overwriteAnnotations) {
            queryOptions.add(VariantDBAdaptor.ANNOTATION_EXISTS, false);
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
            annotationFile = variantAnnotationManager.createAnnotation(outDir, annotateVariantsCommandOptions.fileName == null ? annotateVariantsCommandOptions.dbName : annotateVariantsCommandOptions.fileName, queryOptions);
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
