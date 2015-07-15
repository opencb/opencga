package org.opencb.opencga.storage.core.variant.io;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.*;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfDataWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by jmmut on 2015-06-25.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VariantExporter {
    /**
     * uses a reader and a writer to dump a vcf.
     * TODO jmmut: variantDBReader cannot get the header
     * TODO jmmut: use studyConfiguration to know the order of 
     * 
     * @param adaptor
     * @param studyConfiguration
     * @param outputUri
     * @param options
     */
    public void vcfExport(VariantDBAdaptor adaptor, StudyConfiguration studyConfiguration, URI outputUri, QueryOptions options) {

        // Default objects
        VariantDBReader reader = new VariantDBReader(studyConfiguration, adaptor, options);
        VariantVcfDataWriter writer = new VariantVcfDataWriter(reader, outputUri.getPath());
        int batchSize = 100;

        // user tuning
//        if (options != null) {
//            batchSize = options.getInt(VariantStorageManager.BATCH_SIZE, batchSize);
//        }

        // setup
        reader.open();
        reader.pre();
        writer.open();
        writer.pre();
        
        // actual loop
        List<Variant> variants;
        while (!(variants = reader.read(batchSize)).isEmpty()) {
            writer.write(variants);
        }

        // tear down
        reader.post();
        reader.close();
        writer.post();
        writer.close();
    }

    public void VcfHtsExport(VariantDBAdaptor adaptor, StudyConfiguration studyConfiguration, URI outputUri, QueryOptions options) {
        // Default objects
        VariantDBReader reader = new VariantDBReader(studyConfiguration, adaptor, options);
//        VariantVcfDataWriter writer = new VariantVcfDataWriter(reader, outputUri.getPath());
        VariantContextWriterBuilder builder = new VariantContextWriterBuilder();

        final VCFHeader header = new VCFHeader(new VCFFileReader(new File("/home/jmmut/Documents/biofiles/small.vcf"), false).getFileHeader());
        final SAMSequenceDictionary sequenceDictionary = header.getSequenceDictionary();

        VariantContextWriter writer = builder
                .setOutputFile(outputUri.getPath())
                .setReferenceDictionary(sequenceDictionary)
                .unsetOption(Options.INDEX_ON_THE_FLY)
                .build();

        int batchSize = 100;

        // user tuning
//        if (options != null) {
//            batchSize = options.getInt(VariantStorageManager.BATCH_SIZE, batchSize);
//        }

        // setup
        reader.open();
        reader.pre();
        writer.writeHeader(header);
//        writer.open();
//        writer.pre();

        // actual loop
        List<Variant> variants;
        int failedVariants = 0;
        while (!(variants = reader.read(batchSize)).isEmpty()) {
            for (Variant variant : variants) {
                try {
                    VariantContext variantContext = convertBiodataVariantToVariantContext(variant);
                    writer.add(variantContext);
                } catch (Exception e) {
                    failedVariants++;
                }
            }
        }

        if (failedVariants > 0) {
            Logger logger = LoggerFactory.getLogger(this.getClass().toString());
            logger.warn(failedVariants + " variants were not written due to errors");
        }

        reader.post();
        reader.pre();
        writer.close();
    }

    public VariantContext convertBiodataVariantToVariantContext(Variant variant) {//, StudyConfiguration studyConfiguration) {
        VariantContextBuilder variantContextBuilder = new VariantContextBuilder();
//        Allele refAllele = new Allele(variant.getReference(), true);  // protected constructor...
//        Allele altAllele = new Allele(variant.getAlternate(), false);  // TODO jmmut: multiallelic

        String reference = variant.getReference();
        String alternate = variant.getAlternate();
        List<String> allelesArray = Arrays.asList(reference, alternate);  // TODO jmmut: multiallelic
        ArrayList<Genotype> genotypes = new ArrayList<>();
        for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
            Map<String, Map<String, String>> samplesData = variantSourceEntry.getSamplesData();
            for (Map.Entry<String, Map<String, String>> samplesEntry : samplesData.entrySet()) {
                String gt = samplesEntry.getValue().get("GT");
                if (gt != null) {
                    org.opencb.biodata.models.feature.Genotype genotype = new org.opencb.biodata.models.feature.Genotype(gt, reference, alternate);
                    List<Allele> alleles = new ArrayList<>();
                    for (int gtIdx : genotype.getAllelesIdx()) {
                        alleles.add(Allele.create(allelesArray.get(gtIdx), gtIdx == 0));    // allele is reference if the alleleIndex is 0
                    }
                    genotypes.add(GenotypeBuilder.create(samplesEntry.getKey(), alleles));
                }
            }
        }
        List<String> alleles = Arrays.asList(variant.getReference(), variant.getAlternate());

        VariantContext variantContext = variantContextBuilder.start(variant.getStart())
                .stop(variant.getEnd())
                .chr(variant.getChromosome())
                .alleles(alleles)
                .genotypes(genotypes)
//                .attributes(variant.)// TODO jmmut: join attributes from different source entries? what to do on a collision?
                .make();

        return variantContext;
    }
}

