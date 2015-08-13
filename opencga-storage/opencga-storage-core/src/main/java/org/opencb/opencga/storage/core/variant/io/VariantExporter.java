package org.opencb.opencga.storage.core.variant.io;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.*;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfDataWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.cellbase.core.client.CellBaseClient;
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
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by jmmut on 2015-06-25.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VariantExporter {

    private static final Logger logger = LoggerFactory.getLogger(VariantExporter.class);
    private static final String ORI = "ori";    // attribute present in the variant to retrieve the reference base in indels. Reference base as T in TA	T

    private static CellBaseClient cellbaseClient;

    static {
        try {
            cellbaseClient = new CellBaseClient("bioinfo.hpc.cam.ac.uk", 80, "/cellbase/webservices/rest/", "v3", "hsapiens");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }    }

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
        header.addMetaDataLine(new VCFFilterHeaderLine("PASS", "Valid variant"));
        header.addMetaDataLine(new VCFFilterHeaderLine(".", "No FILTER info"));

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
                if (variantContext != null) {
                    writer.add(variantContext);
                }
            } catch (Exception e) {
                e.printStackTrace();
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

        int start = variant.getStart();
        int end = variant.getEnd();
        String indelSequence;
//        if (reference.isEmpty()) {
//            try {
//                QueryResponse<QueryResult<GenomeSequenceFeature>> resultQueryResponse = cellbaseClient.getSequence(
//                        CellBaseClient.Category.genomic,
//                        CellBaseClient.SubCategory.region,
//                        Arrays.asList(Region.parseRegion(variant.getChromosome() + ":" + start + "-" + start)),
//                        new QueryOptions());
//                indelSequence = resultQueryResponse.getResponse().get(0).getResult().get(0).getSequence();
//                reference = indelSequence;
//                alternate = indelSequence + alternate;
//                end = start + reference.length() - 1;
////                if ((end - start) != reference.length()) {
////                    end = start + reference.length() - 1;
////                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        if (alternate.isEmpty()) {
//            try {
//                start -= reference.length();
//                QueryResponse<QueryResult<GenomeSequenceFeature>> resultQueryResponse = cellbaseClient.getSequence(
//                        CellBaseClient.Category.genomic,
//                        CellBaseClient.SubCategory.region,
//                        Arrays.asList(Region.parseRegion(variant.getChromosome() + ":" + start + "-" + start)),
//                        new QueryOptions());
//                indelSequence = resultQueryResponse.getResponse().get(0).getResult().get(0).getSequence();
//                reference = indelSequence + reference;
//                alternate = indelSequence;
//                if ((end - start) != reference.length()) {
//                    end = start + reference.length() - 1;
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }

        List<String> allelesArray = Arrays.asList(reference, alternate);  // TODO jmmut: multiallelic
        ArrayList<Genotype> genotypes = new ArrayList<>();
        Integer originalPosition = null;
        List<String> originalAlleles = null;
        for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
            String[] ori = getOri(variantSourceEntry);
            Integer auxOriginalPosition = getOriginalPosition(ori);
            if (originalPosition != null && auxOriginalPosition != null && !originalPosition.equals(auxOriginalPosition)) {
                throw new IllegalStateException("Two or more VariantSourceEntries have different origin. Unable to merge");
            }
            originalPosition = auxOriginalPosition;
            originalAlleles = getOriginalAlleles(ori);
            if (originalAlleles == null) {
                originalAlleles = allelesArray;
            }

            //Only print those variants in which the alternate is the first alternate from the multiallelic alternatives
            if (originalAlleles.size() > 2 && !"0".equals(getOriginalAlleleIndex(ori))) {
                logger.debug("Skip multiallelic variant! " + variant);
                return null;
            }


            String sourceFilter = variantSourceEntry.getAttribute("FILTER");
            if (sourceFilter != null && !filter.equals(sourceFilter)) {
                filter = ".";   // write PASS iff all sources agree that the filter is "PASS" or assumed if not present, otherwise write "."
            }

            Map<String, Map<String, String>> samplesData = variantSourceEntry.getSamplesData();
            for (Map.Entry<String, Map<String, String>> samplesEntry : samplesData.entrySet()) {
                String gt = samplesEntry.getValue().get("GT");
//                System.out.println("gt = " + gt);
                if (gt != null) {
                    org.opencb.biodata.models.feature.Genotype genotype = new org.opencb.biodata.models.feature.Genotype(gt, reference, alternate);
                    List<Allele> alleles = new ArrayList<>();
//                    System.out.println("\tgenotype = " + genotype.getReference().toString());
//                    System.out.println("\tgenotype = " + genotype.getAlternate().toString());
                    for (int gtIdx : genotype.getAllelesIdx()) {
//                        System.out.println("\t\t>>"+originalAlleles);
//                        System.out.println("\t\t>>"+gtIdx);
//                        System.out.println("\t\t>>"+originalAlleles.get(gtIdx));
//                        String allele = originalAlleles.get(gtIdx);
//                        if (allele == null || allele.isEmpty()) {
//                            allele = "A";
//                        }
                        if (gtIdx < originalAlleles.size() && gtIdx >= 0) {
                            alleles.add(Allele.create(originalAlleles.get(gtIdx), gtIdx == 0));    // allele is reference if the alleleIndex is 0
//                            alleles.add(Allele.create(allele, gtIdx == 0));    // allele is reference if the alleleIndex is 0
                        } else {
                            alleles.add(Allele.create(".", false)); // genotype of a secondary alternate, or an actual missing
                        }
                    }
                    genotypes.add(new GenotypeBuilder().name(samplesEntry.getKey()).alleles(alleles).phased(genotype.isPhased()).make());
                }
            }
        }

        Set<String> ids = variant.getIds();

        variantContextBuilder.start(originalPosition == null ? start : originalPosition)
                .stop((originalPosition == null ? start : originalPosition) + (originalAlleles == null? allelesArray : originalAlleles).get(0).length() - 1)
                .chr(variant.getChromosome())
                .alleles(originalAlleles == null? allelesArray : originalAlleles)
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
     * assumes that ori is in the form "POS:REF:ALT_0(,ALT_N)*:ALT_IDX"
     * ALT_N is the n-th allele if this is the n-th variant resultant of a multiallelic vcf row
     * @return
     * @param ori
     */
    private static List<String> getOriginalAlleles(String[] ori) {
        if (ori != null && ori.length == 4) {
            String[] multiAllele = ori[2].split(",");
            if (multiAllele.length != 1) {
                ArrayList<String> alleles = new ArrayList<>(multiAllele.length + 1);
                alleles.add(ori[1]);
                alleles.addAll(Arrays.asList(multiAllele));
                return alleles;
            } else {
                return Arrays.asList(ori[1], ori[2]);
            }
        }

        return null;
    }

    private static String getOriginalAlleleIndex(String[] ori) {
        if (ori != null && ori.length == 4) {
            return ori[3];
        }
        return null;
    }

    /**
     * assumes that ori is in the form "POS:REF:ALT_0(,ALT_N)*:ALT_IDX"
     * @return
     * @param ori
     */
    private static Integer getOriginalPosition(String[] ori) {

        if (ori != null && ori.length == 4) {
            return Integer.parseInt(ori[0]);
        }

        return null;
    }

    private static String[] getOri(VariantSourceEntry variantSourceEntry) {

        for (Map.Entry<String, String> entry : variantSourceEntry.getAttributes().entrySet()) {
            if (entry.getKey().endsWith(ORI)) {
                return entry.getValue().split(":");
            }
        }
        return null;
    }


}

