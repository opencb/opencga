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

    private static final Logger logger = LoggerFactory.getLogger(VariantExporter.class);
    private static final String ORI = "ori";    // attribute present in the variant to retrieve the reference base in indels. Reference base as T in TA	T

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
     * @return num variants not written due to errors
     * @throws Exception
     */
    public static int VcfHtsExport(VariantDBIterator iterator, StudyConfiguration studyConfiguration,
                                   OutputStream outputStream, QueryOptions options) throws Exception {
        final VCFHeader header = getVcfHeader(studyConfiguration, options);

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
        return failedVariants;
    }

    private static VCFHeader getVcfHeader(StudyConfiguration studyConfiguration, QueryOptions options) throws Exception {
        //        get header from studyConfiguration
        Collection<String> headers = studyConfiguration.getHeaders().values();
        String returnedSamplesString = null;
        if (options != null) {
            returnedSamplesString = options.getString(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), null);
        }
        String[] returnedSamples = null;
        if (returnedSamplesString != null) {
            returnedSamples = returnedSamplesString.split(",");
        }
        if (headers.size() < 1) {
            throw new Exception("file headers not available for study " + studyConfiguration.getStudyName()
                    + ". note: check files: " + studyConfiguration.getFileIds().values().toString());
        }
        String fileHeader = headers.iterator().next();

        if (headers.size() > 1 || returnedSamples != null) {
            String[] lines = fileHeader.split("\n");
            Set<String> sampleSet = returnedSamples != null?
                    new HashSet<>(Arrays.asList(returnedSamples))
                    : studyConfiguration.getSampleIds().keySet();
            String samples = String.join("\t", sampleSet);
            lines[lines.length-1] = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + samples;
            logger.debug("export will be done on samples: [{}]", samples);

            FileWriter fileWriter = new FileWriter("/tmp/header.vcf");
            for (String line : lines) {
                fileWriter.write(line);
                fileWriter.write("\n");
            }
            fileWriter.close();
        } else {
            FileWriter fileWriter = new FileWriter("/tmp/header.vcf");
            fileWriter.write(fileHeader);
            fileWriter.close();
        }

        // BIG TODO jmmut: build header line by line with VCFHeaderLine and key, value pairs
        return new VCFHeader(new VCFFileReader(new File("/tmp/header.vcf"), false).getFileHeader());
    }

    /**
     * converts org.opencb.biodata.models.variant.Variant into a htsjdk.variant.variantcontext.VariantContext
     * some assumptions:
     * * splitted multiallelic variants will go to different variantContexts, no merging is done
     * * if the variant is an INDEL, the source entries in it may have an attribute ORI with the REF and ALT alleles with reference bases
     * @param variant
     * @return
     */
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

            List<String> originalAlleles = getOriginalAlleles(variantSourceEntry);
            if (originalAlleles == null) {
                originalAlleles = allelesArray;
            }
            Map<String, Map<String, String>> samplesData = variantSourceEntry.getSamplesData();
            for (Map.Entry<String, Map<String, String>> samplesEntry : samplesData.entrySet()) {
                String gt = samplesEntry.getValue().get("GT");
                if (gt != null) {
                    org.opencb.biodata.models.feature.Genotype genotype = new org.opencb.biodata.models.feature.Genotype(gt, reference, alternate);
                    List<Allele> alleles = new ArrayList<>();
                    for (int gtIdx : genotype.getAllelesIdx()) {
                        if (gtIdx < originalAlleles.size()) {
                            alleles.add(Allele.create(originalAlleles.get(gtIdx), gtIdx == 0));    // allele is reference if the alleleIndex is 0
                        } else {
                            alleles.add(Allele.create(".", false)); // genotype of a secondary alternate,
                        }
                    }
                    genotypes.add(new GenotypeBuilder().name(samplesEntry.getKey()).alleles(alleles).phased(genotype.isPhased()).make());
                }
            }
        }

        Set<String> ids = variant.getIds();

        variantContextBuilder.start(variant.getStart())
                .stop(variant.getEnd())
                .chr(variant.getChromosome())
                .alleles(allelesArray)
                .filter(filter)
                .genotypes(genotypes);
//                .attributes(variant.)// TODO jmmut: join attributes from different source entries? what to do on a collision?


        if (ids != null) {
            Optional<String> reduce = variant.getIds().stream().reduce((left, right) -> left + "," + right);
            if (reduce.isPresent()) {
                variantContextBuilder.id(reduce.get());
            }
        }

        return variantContextBuilder.make();
    }

    /**
     * assumes that ori is in the form "REF	ALT_N"
     * ALT_N is the n-th allele if this is the n-th variant resultant of a multiallelic vcf row
     * @param variantSourceEntry
     * @return
     */
    private static List<String> getOriginalAlleles(VariantSourceEntry variantSourceEntry) {

        if (variantSourceEntry.hasAttribute(ORI)) {
            String[] refAlt = variantSourceEntry.getAttribute(ORI).split("\t");

            if (refAlt.length == 2) {
                return Arrays.asList(refAlt[0], refAlt[1]);
            }
        }

        return null;
    }
}

