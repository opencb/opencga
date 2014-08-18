package org.opencb.opencga.storage.core.variant;

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
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantDBWriter;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantEffectTask;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageManager /*implements StorageManager<VariantReader, VariantWriter, VariantDBAdaptor>*/{

    protected Properties properties;

    public VariantStorageManager() {
        properties = new Properties();
    }

    public VariantStorageManager(Path properties) {
        this.properties = new Properties();
        setProperties(properties);
    }

    public void setProperties(Path propertiesPath){
        try {
            properties.load(new InputStreamReader(new FileInputStream(propertiesPath.toString())));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setProperty(String key, String value){
        properties.put(key, value);
    }

    abstract public VariantDBAdaptor getVariantDBAdaptor(Path credentials);

    public VariantReader getVariantDBSchemaReader() {
        return null;
    }

    public List<VariantWriter> getVariantDBSchemaWriter() {
        return null;
    }

    //TODO: Remove VariantSource
    abstract public VariantWriter getVariantDBWriter(Path credentials, VariantSource source);


    /**
     * Transform raw variant files into Biodata model.
     *
     * @param input         Input file. Accepted formats: *.vcf, *.vcf.gz
     * @param pedigree      Pedigree input file. Accepted formats: *.ped
     * @param output
     * @param params
     * @throws IOException
     */
    final public void transform(Path input, Path pedigree, Path output, Map<String, Object> params) throws IOException {
        // input: VcfReader
        // output: JsonWriter

        boolean includeSamples = Boolean.parseBoolean(params.get("includeSamples").toString());
        boolean includeEffect = Boolean.parseBoolean(params.get("includeEffect").toString());
        boolean includeStats = Boolean.parseBoolean(params.get("includeStats").toString());
        String aggregated = (String) params.get("aggregated");
        VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());

        //Reader
        VariantReader reader;
        PedigreeReader pedReader = null;
        if(pedigree != null && pedigree.toFile().exists()) {    //FIXME Add "endsWith(".ped") ??
            pedReader = new PedigreePedReader(pedigree.toString());
        }

        // TODO Create a utility to determine which extensions are variants files
        if (source.getFileName().endsWith(".vcf") || source.getFileName().endsWith(".vcf.gz")) {
            if (aggregated != null) {
                includeStats = false;
                switch (aggregated.toLowerCase()) {
                    case "basic":
                        reader = new VariantVcfReader(source, input.toAbsolutePath().toString(), new VariantAggregatedVcfFactory());
                        break;
                    case "evs":
                        reader = new VariantVcfReader(source, input.toAbsolutePath().toString(), new VariantVcfEVSFactory());
                        break;
                    default:
                        reader = new VariantVcfReader(source, input.toAbsolutePath().toString());
                        break;

                }
            } else {
                reader = new VariantVcfReader(source, input.toAbsolutePath().toString());
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

        System.out.println("Indexing variants...");
        vr.run();
        System.out.println("Variants indexed!");
    }


    public void preLoad(Path input, Path output, Map<String, Object> params) throws IOException {
        // input: JsonVariatnReader
        // output: getVariantDBSchemaWriter

        //Writers
        List<VariantWriter> writers = this.getVariantDBSchemaWriter();
        if(writers == null){
            System.out.println("[ALERT] preLoad method not supported in this plugin");
            return;
        }

        VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());

        //Reader
        String sourceFile = input.toAbsolutePath().toString().replace("variant.json", "file.json");
        VariantReader jsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString() , sourceFile);


        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();


        //Runner
        VariantRunner vr = new VariantRunner(source, jsonReader, null, writers, taskList);

        System.out.println("Indexing variants...");
        vr.run();
        System.out.println("Variants indexed!");

    }


    public void load(Path input, Path credentials, Map<String, Object> params) throws IOException {
        // input: getVariantDBSchemaReader
        // output: getVariantDBWriter()

        boolean includeSamples = Boolean.parseBoolean(params.get("includeSamples").toString());
        boolean includeEffect = Boolean.parseBoolean(params.get("includeEffect").toString());
        boolean includeStats = Boolean.parseBoolean(params.get("includeStats").toString());
        VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());

        //Reader
        VariantReader variantDBSchemaReader = this.getVariantDBSchemaReader();
        if (variantDBSchemaReader == null) {
            if (source.getFileName().endsWith(".json") || source.getFileName().endsWith(".json.gz")) {
                String sourceFile = input.toAbsolutePath().toString().replace("variant.json", "file.json");
                variantDBSchemaReader = new VariantJsonReader(source, input.toAbsolutePath().toString(), sourceFile);
            } else {
                throw new IOException("Variants input file format not supported");
            }
        }

        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();


        //Writers
        VariantWriter variantDBWriter = this.getVariantDBWriter(credentials, source);
        List<VariantWriter> writers = new ArrayList<>();
        writers.add(variantDBWriter);

        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(includeSamples);
            variantWriter.includeEffect(includeEffect);
            variantWriter.includeStats(includeStats);
        }

        //Runner
        VariantRunner vr = new VariantRunner(source, variantDBSchemaReader, null, writers, taskList);

        System.out.println("Indexing variants...");
        vr.run();
        System.out.println("Variants indexed!");
    }


}
