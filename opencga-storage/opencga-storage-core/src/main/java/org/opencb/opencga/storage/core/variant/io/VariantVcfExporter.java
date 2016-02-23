package org.opencb.opencga.storage.core.variant.io;

import com.google.common.collect.BiMap;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfDataWriter;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ClinVar;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.Cosmic;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.GeneDrugInteraction;
import org.opencb.biodata.models.variant.avro.Gwas;
import org.opencb.biodata.models.variant.avro.Score;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.tools.variant.converter.VariantFileMetadataToVCFHeaderConverter;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jmmut on 2015-06-25.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VariantVcfExporter {

    private static final Logger logger = LoggerFactory.getLogger(VariantVcfExporter.class);
//    private static final String ORI = "ori";    // attribute present in the variant to retrieve the reference base in indels. Reference base as T in TA	T


    private static String DEFAULT_ANNOTATIONS = "allele|gene|ensemblGene|ensemblTranscript|biotype|consequenceType|phastCons|phylop" +
            "|populationFrequency|cDnaPosition|cdsPosition|proteinPosition|sift|polyphen|clinvar|cosmic|gwas|drugInteraction";

    private static String ALL_ANNOTATIONS = "allele|gene|ensemblGene|ensemblTranscript|biotype|consequenceType|phastCons|phylop" +
            "|populationFrequency|cDnaPosition|cdsPosition|proteinPosition|sift|polyphen|clinvar|cosmic|gwas|drugInteraction";

    private DecimalFormat df3 = new DecimalFormat("#.###");

//    static {
//        try {
//            cellbaseClient = new CellBaseClient("bioinfo.hpc.cam.ac.uk", 80, "/cellbase/webservices/rest/", "v3", "hsapiens");
//        } catch (URISyntaxException e) {
//            e.printStackTrace();
//        }
//    }

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
    public static void vcfExport(VariantDBAdaptor adaptor, StudyConfiguration studyConfiguration, URI outputUri, Query query, QueryOptions options) {

        // Default objects
        VariantDBReader reader = new VariantDBReader(studyConfiguration, adaptor, query, options);
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
     * @param queryOptions TODO fill
     * @return num variants not written due to errors
     * @throws Exception
     */
    public int export(VariantDBIterator iterator, StudyConfiguration studyConfiguration, OutputStream outputStream,
                      QueryOptions queryOptions) throws Exception {

        final VCFHeader header = getVcfHeader(studyConfiguration, queryOptions);
        header.addMetaDataLine(new VCFFilterHeaderLine("PASS", "Valid variant"));
        header.addMetaDataLine(new VCFFilterHeaderLine(".", "No FILTER info"));

        // check if variant annotations are exported in the INFO column
        List<String> annotations = null;
        if (queryOptions != null && queryOptions.getString("annotations") != null && !queryOptions.getString("annotations").isEmpty()) {
            String annotationString;
            switch (queryOptions.getString("annotations")) {
                case "all":
                    annotationString = ALL_ANNOTATIONS.replaceAll(",", "|");
                    break;
                case "default":
                    annotationString = DEFAULT_ANNOTATIONS.replaceAll(",", "|");
                    break;
                default:
                    annotationString = queryOptions.getString("annotations").replaceAll(",", "|");
                    break;
            }
//            String annotationString = queryOptions.getString("annotations", DEFAULT_ANNOTATIONS).replaceAll(",", "|");
            annotations = Arrays.asList(annotationString.split("\\|"));
            header.addMetaDataLine(new VCFInfoHeaderLine("CSQ", 1, VCFHeaderLineType.String, "Consequence annotations from CellBase. Format: " + annotationString));
        }

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
                VariantContext variantContext = convertVariantToVariantContext(variant, annotations);
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

    private VCFHeader getVcfHeader(StudyConfiguration studyConfiguration, QueryOptions options) throws Exception {
        //        get header from studyConfiguration
        Collection<String> headers = studyConfiguration.getHeaders().values();
        List<String> returnedSamples = new ArrayList<>();
        if (options != null) {
            returnedSamples = options.getAsStringList(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key());
        }
        if (headers.size() < 1) {
            throw new Exception("file headers not available for study " + studyConfiguration.getStudyName()
                    + ". note: check files: " + studyConfiguration.getFileIds().values().toString());
        }
        String fileHeader = headers.iterator().next();

        int lastLineIndex = fileHeader.lastIndexOf("#CHROM");
        if (lastLineIndex >= 0) {
            String substring = fileHeader.substring(0, lastLineIndex);
            if (returnedSamples.isEmpty()) {
                BiMap<Integer, String> samplesPosition = StudyConfiguration.getIndexedSamplesPosition(studyConfiguration).inverse();
                returnedSamples = new ArrayList<>(samplesPosition.size());
                for (int i = 0; i < samplesPosition.size(); i++) {
                    returnedSamples.add(samplesPosition.get(i));
                }
            }
            String samples = String.join("\t", returnedSamples);
            logger.debug("export will be done on samples: [{}]", samples);

            fileHeader = substring + "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + samples;
        }

        return VariantFileMetadataToVCFHeaderConverter.parseVcfHeader(fileHeader);
    }

    /**
     * converts org.opencb.biodata.models.variant.Variant into a htsjdk.variant.variantcontext.VariantContext
     * some assumptions:
     * * splitted multiallelic variants will produce only one variantContexts. Merging is done
     * * If some normalization have been done to the variant, the source entries may have an attribute ORI like: "POS:REF:ALT_0(,ALT_N)*:ALT_IDX"
     * @param variant
     * @return
     */
    public VariantContext convertVariantToVariantContext(Variant variant, List<String> annotations) {//, StudyConfiguration studyConfiguration) {

        VariantContextBuilder variantContextBuilder = new VariantContextBuilder();

        int start = variant.getStart();
        int end = variant.getEnd();
        String reference = variant.getReference();
        String alternate = variant.getAlternate();
        String filter = "PASS";
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
        for (StudyEntry studyEntry : variant.getStudies()) {
            String[] ori = getOri(studyEntry);
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
                logger.debug("Skip multi allelic variant! " + variant);
                return null;
            }

            String sourceFilter = studyEntry.getAttribute("FILTER");
            if (sourceFilter != null && !filter.equals(sourceFilter)) {
                filter = ".";   // write PASS iff all sources agree that the filter is "PASS" or assumed if not present, otherwise write "."
            }

            for (String sampleName : studyEntry.getOrderedSamplesName()) {
                Map<String, String> sampleData = studyEntry.getSampleData(sampleName);
                String gt = sampleData.get("GT");
                if (gt != null) {
                    org.opencb.biodata.models.feature.Genotype genotype = new org.opencb.biodata.models.feature.Genotype(gt, reference, alternate);
                    List<Allele> alleles = new ArrayList<>();
                    for (int gtIdx : genotype.getAllelesIdx()) {
                        if (gtIdx < originalAlleles.size() && gtIdx >= 0) {
                            alleles.add(Allele.create(originalAlleles.get(gtIdx), gtIdx == 0));    // allele is reference if the alleleIndex is 0
                        } else {
                            alleles.add(Allele.create(".", false)); // genotype of a secondary alternate, or an actual missing
                        }
                    }
                    genotypes.add(new GenotypeBuilder().name(sampleName).alleles(alleles).phased(genotype.isPhased()).make());
                }
            }
        }

        List<String> ids = variant.getIds();


        variantContextBuilder.start(originalPosition == null ? start : originalPosition)
                .stop((originalPosition == null ? start : originalPosition) + (originalAlleles == null ? allelesArray : originalAlleles).get(0).length() - 1)
                .chr(variant.getChromosome())
                .alleles(originalAlleles == null ? allelesArray : originalAlleles)
                .filter(filter)
                .genotypes(genotypes); // TODO jmmut: join attributes from different source entries? what to do on a collision?

        // if asked variant annotations are exported
        if (annotations != null) {
            Map<String, Object> infoAnnotations = getAnnotations(variant, annotations);
            variantContextBuilder.attributes(infoAnnotations);
        }


        if (ids != null) {
            Optional<String> reduce = variant.getIds().stream().reduce((left, right) -> left + "," + right);
            if (reduce.isPresent()) {
                variantContextBuilder.id(reduce.get());
            }
        }

        return variantContextBuilder.make();
    }

    private Map<String, Object> getAnnotations(Variant variant, List<String> annotations) {
        Map<String, Object> infoAnnotations = new HashMap<>();
        StringBuilder stringBuilder = new StringBuilder();
        if (variant.getAnnotation() == null) {
            return infoAnnotations;
        }
//        for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
        for (int i = 0; i < variant.getAnnotation().getConsequenceTypes().size(); i++) {
            ConsequenceType consequenceType = variant.getAnnotation().getConsequenceTypes().get(i);
            for (int j = 0; j < annotations.size(); j++) {
                switch (annotations.get(j)) {
                    case "allele":
                        stringBuilder.append(variant.getAlternate());
                        break;
                    case "consequenceType":
                        stringBuilder.append(consequenceType.getSequenceOntologyTerms().stream()
                                .map(SequenceOntologyTerm::getName).collect(Collectors.joining(",")));
                        break;
                    case "gene":
                        stringBuilder.append(consequenceType.getGeneName());
                        break;
                    case "ensemblGene":
                        stringBuilder.append(consequenceType.getEnsemblGeneId());
                        break;
                    case "ensemblTranscript":
                        stringBuilder.append(consequenceType.getEnsemblTranscriptId());
                        break;
                    case "biotype":
                        stringBuilder.append(consequenceType.getBiotype());
                        break;
                    case "phastCons":
                        List<Double> phastCons = variant.getAnnotation().getConservation().stream()
                                .filter(t -> t.getSource().equalsIgnoreCase("phastCons"))
                                .map(Score::getScore)
                                .collect(Collectors.toList());
                        if (phastCons.size() > 0) {
                            stringBuilder.append(df3.format(phastCons.get(0)));
                        }
                        break;
                    case "phylop":
                        List<Double> phylop = variant.getAnnotation().getConservation().stream()
                                .filter(t -> t.getSource().equalsIgnoreCase("phylop"))
                                .map(Score::getScore)
                                .collect(Collectors.toList());
                        if (phylop.size() > 0) {
                            stringBuilder.append(df3.format(phylop.get(0)));
                        }
                        break;
                    case "populationFrequency":
                        stringBuilder.append(variant.getAnnotation().getPopulationFrequencies().stream()
                                .map(t -> t.getPopulation() + ":" + t.getAltAlleleFreq())
                                .collect(Collectors.joining(",")));
                        break;
                    case "cDnaPosition":
                        stringBuilder.append(consequenceType.getCdnaPosition());
                        break;
                    case "cdsPosition":
                        stringBuilder.append(consequenceType.getCdsPosition());
                        break;
                    case "proteinPosition":
                        stringBuilder.append(consequenceType.getProteinVariantAnnotation().getPosition());
                        break;
                    case "sift":
                        List<Double> sift = consequenceType.getProteinVariantAnnotation().getSubstitutionScores().stream()
                                .filter(t -> t.getSource().equalsIgnoreCase("sift"))
                                .map(Score::getScore)
                                .collect(Collectors.toList());
                        if (sift.size() > 0) {
                            stringBuilder.append(df3.format(sift.get(0)));
                        }
                        break;
                    case "polyphen":
                        List<Double> polyphen = consequenceType.getProteinVariantAnnotation().getSubstitutionScores().stream()
                                .filter(t -> t.getSource().equalsIgnoreCase("polyphen"))
                                .map(Score::getScore)
                                .collect(Collectors.toList());
                        if (polyphen.size() > 0) {
                            stringBuilder.append(df3.format(polyphen.get(0)));
                        }
                        break;
                    case "clinvar":
                        if(variant.getAnnotation().getVariantTraitAssociation() != null
                                && variant.getAnnotation().getVariantTraitAssociation().getClinvar() != null) {
                            stringBuilder.append(variant.getAnnotation().getVariantTraitAssociation().getClinvar().stream()
                                    .map(ClinVar::getTraits).flatMap(Collection::stream)
                                    .collect(Collectors.joining(",")));
                        }
                        break;
                    case "cosmic":
                        if(variant.getAnnotation().getVariantTraitAssociation() != null
                                && variant.getAnnotation().getVariantTraitAssociation().getCosmic() != null) {
                            stringBuilder.append(variant.getAnnotation().getVariantTraitAssociation().getCosmic().stream()
                                    .map(Cosmic::getPrimarySite)
                                    .collect(Collectors.joining(",")));
                        }
                        break;
                    case "gwas":
                        if(variant.getAnnotation().getVariantTraitAssociation() != null
                                && variant.getAnnotation().getVariantTraitAssociation().getGwas() != null) {
                            stringBuilder.append(variant.getAnnotation().getVariantTraitAssociation().getGwas().stream()
                                    .map(Gwas::getTraits).flatMap(Collection::stream)
                                    .collect(Collectors.joining(",")));
                        }
                        break;
                    case "drugInteraction":
                        stringBuilder.append(variant.getAnnotation().getGeneDrugInteraction().stream()
                                .map(GeneDrugInteraction::getDrugName).collect(Collectors.joining(",")));
                        break;
                    default:
                        System.out.println("this is not possible");
                        break;
                }
                if (j < annotations.size() - 1) {
                    stringBuilder.append("|");
                }
            }
            if (i < variant.getAnnotation().getConsequenceTypes().size() - 1) {
                stringBuilder.append("&");
            }
        }

        infoAnnotations.put("CSQ", stringBuilder.toString());
//        infoAnnotations.put("CSQ", stringBuilder.toString().replaceAll("&|$", ""));
        return infoAnnotations;
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

    private static String[] getOri(StudyEntry studyEntry) {

        List<FileEntry> files = studyEntry.getFiles();
        if (!files.isEmpty()) {
            String call = files.get(0).getCall();
            if (call != null && !call.isEmpty()) {
                return call.split(":");
            }
        }
        return null;
    }


}

