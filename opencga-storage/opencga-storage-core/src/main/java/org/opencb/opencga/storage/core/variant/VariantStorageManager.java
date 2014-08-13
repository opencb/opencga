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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageManager {

    protected Properties properties;
    private Path[] props;

    public VariantStorageManager() {

    }

    public VariantStorageManager(Path properties) {

    }


    abstract public VariantDBAdaptor getVariantDBAdaptor();


    public VariantReader getVariantDBSchemaReader() {
        return null;
    }

    public List<VariantWriter> getVariantDBSchemaWriter() {
        return null;
    }

    abstract public VariantWriter getVariantDBWriter();

//    indexVariants("transform", source, variantsPath, pedigreePath, outdir, "json", null, c.includeEffect, c.includeStats, c.includeSamples, c.aggregated);
//private static void indexVariants(String step, VariantSource source, Path mainFilePath, Path auxiliaryFilePath, Path outdir, String backend,
//                                  Path credentialsPath, boolean includeEffect, boolean includeStats, boolean includeSamples, String aggregated)

    final public void transform(Path input, Path pedigree, Path output, Map<String, Object> params) throws IOException {
        // input: VcfReader
        // output: JsonWriter

        Path variantsPath = input;
        Path outdir = output;
        VariantSource source = new VariantSource(variantsPath.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());

        VariantReader reader;
        PedigreeReader pedReader = null;
        if(pedigree != null && pedigree.toFile().exists()) {
            pedReader = new PedigreePedReader(pedigree.toString());
        }

        // TODO Create a utility to determine which extensions are variants files
        if (source.getFileName().endsWith(".vcf") || source.getFileName().endsWith(".vcf.gz")) {
            if (params.get("aggregated") != null) {
                params.put("includeStats", false);
                switch (params.get("aggregated").toString().toLowerCase()) {
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
        } else if (source.getFileName().endsWith(".json") || source.getFileName().endsWith(".json.gz")) {
            assert (pedigree != null);
            reader = new VariantJsonReader(source, input.toAbsolutePath().toString(), pedigree.toAbsolutePath().toString());
        } else {
            throw new IOException("Variants input file format not supported");
        }

        List<Task<Variant>> taskList = new SortedList<>();

        List<VariantWriter> writers = new ArrayList<>();
        writers.add(new VariantJsonWriter(source, outdir));

        // If a JSON file is provided, then stats and effects do not need to be recalculated
        if (!source.getFileName().endsWith(".json") && !source.getFileName().endsWith(".json.gz")) {
            if (Boolean.parseBoolean(params.get("includeEffect").toString())) {
                taskList.add(new VariantEffectTask());
            }

            if (Boolean.parseBoolean(params.get("includeStats").toString())) {
                taskList.add(new VariantStatsTask(reader, source));
            }
        }

        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(Boolean.parseBoolean(params.get("includeSamples").toString()));
            variantWriter.includeEffect(Boolean.parseBoolean(params.get("includeEffect").toString()));
            variantWriter.includeStats(Boolean.parseBoolean(params.get("includeStats").toString()));
        }

        VariantRunner vr = new VariantRunner(source, reader, pedReader, writers, taskList);

        System.out.println("Indexing variants...");
        vr.run();
        System.out.println("Variants indexed!");
    }


    public void preLoad(Path input, Path output) {
        // input: JsonVariatnReader
        // output: getVariantDBSchemaWriter

    }


    public void load(Path input) {
        // input: getVariantDBSchemaReader
        // output: getVariantDBWriter()
        VariantWriter variantDBWriter = this.getVariantDBWriter();

    }


}
