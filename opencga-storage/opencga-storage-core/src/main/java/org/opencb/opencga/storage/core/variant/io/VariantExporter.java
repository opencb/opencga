package org.opencb.opencga.storage.core.variant.io;

import com.fasterxml.jackson.databind.ObjectWriter;
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
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.OutputStream;
import java.net.URI;
import java.util.*;

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
    public static void vcfExport(VariantDBAdaptor adaptor, StudyConfiguration studyConfiguration, URI outputUri, QueryOptions options) {

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

    /**
     * 
     * @param iterator 
     * @param studyConfiguration necessary for the header
     * @param outputStream
     * @param options TODO fill
     * @throws Exception
     */
    public static void VcfHtsExport(VariantDBIterator iterator, StudyConfiguration studyConfiguration, 
                                    OutputStream outputStream, QueryOptions options) throws Exception {
        Logger logger = LoggerFactory.getLogger(VariantExporter.class);
        
//        get header from studyConfiguration
        Collection<String> headers = studyConfiguration.getHeaders().values();
        if (headers.size() < 1) {
            throw new Exception("file headers not available for study " + studyConfiguration.getStudyName() 
                    + ". note: check files: " + studyConfiguration.getFileIds().values().toString());
        }
        if (headers.size() > 1) {
            // TODO build our own header 
            logger.warn("exporting a vcf from several files is unsupported, will use just one header");
        }
        
        String fileHeader = headers.iterator().next();
        FileWriter fileWriter = new FileWriter("/tmp/header.vcf");
        fileWriter.write(fileHeader);
        fileWriter.close();
        
        // BIG TODO jmmut: build header line by line with VCFHeaderLine and key, value pairs
        final VCFHeader header = new VCFHeader(new VCFFileReader(new File("/tmp/header.vcf"), false).getFileHeader()); 
        final SAMSequenceDictionary sequenceDictionary = header.getSequenceDictionary();

        // setup writer
        VariantContextWriterBuilder builder = new VariantContextWriterBuilder();
        VariantContextWriter writer = builder
                .setOutputStream(outputStream)
                .setReferenceDictionary(sequenceDictionary)
                .unsetOption(Options.INDEX_ON_THE_FLY)
                .build();

        writer.writeHeader(header);
        
        // actual loop
        int failedVariants = 0;
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            try {
                VariantContext variantContext = convertBiodataVariantToVariantContext(variant);
                writer.add(variantContext);
            } catch (Exception e) {
                failedVariants++;
            }
        }

        if (failedVariants > 0) {
            logger.warn(failedVariants + " variants were not written due to errors");
        }

        writer.close();
    }

    public static VariantContext convertBiodataVariantToVariantContext(Variant variant) {//, StudyConfiguration studyConfiguration) {
        VariantContextBuilder variantContextBuilder = new VariantContextBuilder();

        String reference = variant.getReference();
        String alternate = variant.getAlternate();
        String filter = "PASS";
        List<String> allelesArray = Arrays.asList(reference, alternate);  // TODO jmmut: multiallelic
        ArrayList<Genotype> genotypes = new ArrayList<>();
        for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
            String sourceFilter = variantSourceEntry.getAttribute("FILTER");
            if (sourceFilter != null && !filter.equals(sourceFilter)) {
                filter = ".";   // write PASS iff all sources agree that the filter is "PASS" or assumed if not present, otherwise write "."
            }
            // TODO add genotypes if multiallelic, and remove them later
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

        Optional<String> reduce = variant.getIds().stream().reduce((left, right) -> left + "," + right);

        variantContextBuilder.start(variant.getStart())
                .stop(variant.getEnd())
                .chr(variant.getChromosome())
                .alleles(allelesArray)
                .filter(filter)
                .genotypes(genotypes);
//                .attributes(variant.)// TODO jmmut: join attributes from different source entries? what to do on a collision?


        if (reduce.isPresent()) {
            variantContextBuilder.id(reduce.get());
        }

        return variantContextBuilder.make();
    }
}

