package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantAggregatedVcfFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfEVSFactory;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantDBWriter;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantEffectTask;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageManager implements StorageManager<VariantWriter, VariantDBAdaptor> {


    public static final String INCLUDE_EFFECT = "includeEffect";
    public static final String INCLUDE_STATS = "includeStats";
    public static final String INCLUDE_SAMPLES = "includeSamples";
    public static final String SOURCE = "source";
    public static final String DB_NAME = "dbName";
    protected Properties properties;
    protected static Logger logger = LoggerFactory.getLogger(VariantStorageManager.class);


    public VariantStorageManager() {
        this.properties = new Properties();
    }

    //@Override
    public void addPropertiesPath(Path propertiesPath){
        if(propertiesPath != null && propertiesPath.toFile().exists()) {
            try {
                properties.load(new InputStreamReader(new FileInputStream(propertiesPath.toString())));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void extract(URI from, URI to, ObjectMap params) {

    }

    @Override
    public void preTransform(URI input, ObjectMap params) throws IOException, FileFormatException {

    }

    /**
     * Transform raw variant files into biodata model.
     *
     * @param inputUri         Input file. Accepted formats: *.vcf, *.vcf.gz
     * @param pedigreeUri      Pedigree input file. Accepted formats: *.ped
     * @param outputUri
     * @param params
     * @throws IOException
     */
    @Override
    final public void transform(URI inputUri, URI pedigreeUri, URI outputUri, ObjectMap params) throws IOException {
        // input: VcfReader
        // output: JsonWriter

        Path input = Paths.get(inputUri.getPath());
        Path pedigree = pedigreeUri == null? null : Paths.get(pedigreeUri.getPath());
        Path output = Paths.get(outputUri.getPath());

        boolean includeSamples = params.getBoolean(INCLUDE_SAMPLES);
        boolean includeEffect = params.getBoolean(INCLUDE_EFFECT);
        boolean includeStats = params.getBoolean(INCLUDE_STATS);
        VariantSource source = params.get(SOURCE, VariantSource.class);
        //VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());

        //Reader
        VariantReader reader = null;
        PedigreeReader pedReader = null;
        if(pedigree != null && pedigree.toFile().exists()) {    //FIXME Add "endsWith(".ped") ??
            pedReader = new PedigreePedReader(pedigree.toString());
        }

        // TODO Create a utility to determine which extensions are variants files
        if (source.getFileName().endsWith(".vcf") || source.getFileName().endsWith(".vcf.gz")) {
            switch (source.getAggregation()) {
                case NONE:
                    reader = new VariantVcfReader(source, input.toAbsolutePath().toString());
                    break;
                case BASIC:
                    reader = new VariantVcfReader(source, input.toAbsolutePath().toString(), new VariantAggregatedVcfFactory());
                    break;
                case EVS:
                    reader = new VariantVcfReader(source, input.toAbsolutePath().toString(), new VariantVcfEVSFactory());
                    break;
            }
        } else {
            throw new IOException("Variants input file format not supported");
        }

        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();
        if (includeEffect) {
            taskList.add(new VariantEffectTask());
        }
        if (includeStats) {
            taskList.add(new VariantStatsTask(reader, source));
        }

        //Writers
        List<VariantWriter> writers = new ArrayList<>();
        writers.add(new VariantJsonWriter(source, output));

        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(includeSamples);
            variantWriter.includeEffect(includeEffect);
            variantWriter.includeStats(includeStats);
        }

        //Runner
        VariantRunner vr = new VariantRunner(source, reader, pedReader, writers, taskList);

        logger.info("Transforming variants...");
        long start = System.currentTimeMillis();
        vr.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants transformed!");
    }

    @Override
    public void postTransform(URI output, ObjectMap params) throws IOException, FileFormatException {

    }

    protected VariantJsonReader getVariantJsonReader(Path input, VariantSource source) throws IOException {
        VariantJsonReader variantJsonReader;
        if (source.getFileName().endsWith(".json") || source.getFileName().endsWith(".json.gz")) {
            String sourceFile = input.toAbsolutePath().toString().replace("variants.json", "file.json");
            variantJsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString(), sourceFile);
        } else {
            throw new IOException("Variants input file format not supported");
        }
        return variantJsonReader;
    }

//    @Override
//    public void preLoad(URI inputUri, URI outputUri, ObjectMap params) throws IOException {
//        // input: JsonVariatnReader
//        // output: getDBSchemaWriter
//
//        Path input = Paths.get(inputUri);
//        Path output = Paths.get(outputUri);
//
//        //Writers
//        VariantWriter dbSchemaWriter = this.getDBSchemaWriter(outputUri);
//        if(dbSchemaWriter == null){
//            System.out.println("[ALERT] preLoad method not supported in this plugin");
//            return;
//        }
//        List<VariantWriter> writers = Arrays.asList(dbSchemaWriter);
//
//        //VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());
//        VariantSource source = (VariantSource) params.get("source");
//
//        //Reader
//        String sourceFile = input.toAbsolutePath().toString().replace("variants.json", "file.json");
//        VariantReader jsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString() , sourceFile);
//
//
//        //Tasks
//        List<Task<Variant>> taskList = new SortedList<>();
//
//
//        //Runner
//        VariantRunner vr = new VariantRunner(source, jsonReader, null, writers, taskList);
//
//        logger.info("Preloading variants...");
//        long start = System.currentTimeMillis();
//        vr.run();
//        long end = System.currentTimeMillis();
//        logger.info("end - start = " + (end - start) / 1000.0 + "s");
//        logger.info("Variants preloaded!");
//
//    }



}
