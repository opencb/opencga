package org.opencb.opencga.storage.app.cli;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Created by imedina on 02/03/15.
 */
public class IndexVariantsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.IndexVariantsCommandOptions indexVariantsCommandOptions;

    public static final String OPENCGA_STORAGE_ANNOTATOR = "OPENCGA.STORAGE.ANNOTATOR";

    public IndexVariantsCommandExecutor(CliOptionsParser.IndexVariantsCommandOptions indexVariantsCommandOptions) {
        super(indexVariantsCommandOptions.logLevel, indexVariantsCommandOptions.verbose,
                indexVariantsCommandOptions.configFile);

        this.indexVariantsCommandOptions = indexVariantsCommandOptions;
    }
    @Override
    public void execute() {
        System.out.println("in IndexVariantsCommandExecutor");
        Config.setOpenCGAHome();
        try {
            VariantStorageManager variantStorageManager;
            String storageEngine = indexVariantsCommandOptions.storageEngine;
            variantStorageManager = StorageManagerFactory.getVariantStorageManager(storageEngine);

            if(indexVariantsCommandOptions.credentials != null && !indexVariantsCommandOptions.credentials.isEmpty()) {
                variantStorageManager.addConfigUri(new URI(null, indexVariantsCommandOptions.credentials, null));
            }

            URI variantsUri = new URI(null, indexVariantsCommandOptions.input, null);
            URI pedigreeUri = indexVariantsCommandOptions.pedigree != null && !indexVariantsCommandOptions.pedigree.isEmpty() ? new URI(null, indexVariantsCommandOptions.pedigree, null) : null;
            URI outdirUri;
            if (indexVariantsCommandOptions.outdir != null && !indexVariantsCommandOptions.outdir.isEmpty()) {
                outdirUri = new URI(null, indexVariantsCommandOptions.outdir + (indexVariantsCommandOptions.outdir.endsWith("/") ? "" : "/"), null).resolve(".");
            } else {
                outdirUri = variantsUri.resolve(".");
            }
//            assertDirectoryExists(outdirUri);

            String fileName = variantsUri.resolve(".").relativize(variantsUri).toString();
            VariantSource source = new VariantSource(fileName, indexVariantsCommandOptions.fileId,
                    indexVariantsCommandOptions.studyId, indexVariantsCommandOptions.study, indexVariantsCommandOptions.studyType, indexVariantsCommandOptions.aggregated);

            ObjectMap params = new ObjectMap();
            params.put(VariantStorageManager.CALCULATE_STATS, indexVariantsCommandOptions.calculateStats);
            params.put(VariantStorageManager.INCLUDE_STATS, indexVariantsCommandOptions.includeStats);
            params.put(VariantStorageManager.INCLUDE_SAMPLES, indexVariantsCommandOptions.includeGenotype);   // TODO rename samples to genotypes
            params.put(VariantStorageManager.INCLUDE_SRC, indexVariantsCommandOptions.includeSrc);
            params.put(VariantStorageManager.COMPRESS_GENOTYPES, indexVariantsCommandOptions.compressGenotypes);
//            params.put(VariantStorageManager.VARIANT_SOURCE, source);
            params.put(VariantStorageManager.DB_NAME, indexVariantsCommandOptions.dbName);
            params.put(VariantStorageManager.ANNOTATE, indexVariantsCommandOptions.annotate);
            params.put(VariantStorageManager.OVERWRITE_ANNOTATIONS, indexVariantsCommandOptions.overwriteAnnotations);

            if(indexVariantsCommandOptions.annotate) {
                //Get annotator config
                Properties annotatorProperties = Config.getStorageProperties();
                if(indexVariantsCommandOptions.annotatorConfigFile != null && !indexVariantsCommandOptions.annotatorConfigFile.isEmpty()) {
                    annotatorProperties.load(new FileInputStream(indexVariantsCommandOptions.annotatorConfigFile));
                }
                params.put(VariantStorageManager.ANNOTATOR_PROPERTIES, annotatorProperties);

                //Get annotation source
                VariantAnnotationManager.AnnotationSource annotatorSource = indexVariantsCommandOptions.annotator;
                if(annotatorSource == null) {
                    annotatorSource = org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource.valueOf(
                            annotatorProperties.getProperty(
                                    OPENCGA_STORAGE_ANNOTATOR,
                                    org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name()
                            ).toUpperCase()
                    );
                }
                params.put(VariantStorageManager.ANNOTATION_SOURCE, annotatorSource);
            }

            params.putAll(indexVariantsCommandOptions.params);

            URI nextFileUri = variantsUri;


            boolean extract, transform, load;

            if (!indexVariantsCommandOptions.load && !indexVariantsCommandOptions.transform) {
                extract = true;
                transform = true;
                load = true;
            } else {
                extract = indexVariantsCommandOptions.transform;
                transform = indexVariantsCommandOptions.transform;
                load = indexVariantsCommandOptions.load;
            }

            if (extract) {
                logger.info("-- Extract variants -- {}", variantsUri);
                nextFileUri = variantStorageManager.extract(variantsUri, outdirUri, params);
            }

            if (transform) {
                logger.info("-- PreTransform variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.preTransform(nextFileUri, params);
                logger.info("-- Transform variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri, params);
                logger.info("-- PostTransform variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.postTransform(nextFileUri, params);
            }

            if (load) {
                logger.info("-- PreLoad variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.preLoad(nextFileUri, outdirUri, params);
                logger.info("-- Load variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.load(nextFileUri, params);
                logger.info("-- PostLoad variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.postLoad(nextFileUri, outdirUri, params);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FileFormatException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageManagerException e) {
            e.printStackTrace();
        }
    }



}
