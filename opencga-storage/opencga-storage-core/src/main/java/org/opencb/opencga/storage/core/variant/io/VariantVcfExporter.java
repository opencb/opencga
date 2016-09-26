package org.opencb.opencga.storage.core.variant.io;

import com.google.common.collect.BiMap;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.*;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfDataWriter;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.opencb.biodata.tools.variant.converter.VariantFileMetadataToVCFHeaderConverter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jmmut on 2015-06-25.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VariantVcfExporter implements DataWriter<Variant> {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.#######");
    private final Logger logger = LoggerFactory.getLogger(VariantVcfExporter.class);


    private static final String DEFAULT_ANNOTATIONS = "allele|gene|ensemblGene|ensemblTranscript|biotype|consequenceType|phastCons|phylop"
            + "|populationFrequency|cDnaPosition|cdsPosition|proteinPosition|sift|polyphen|clinvar|cosmic|gwas|drugInteraction";

    private static final String ALL_ANNOTATIONS = "allele|gene|ensemblGene|ensemblTranscript|biotype|consequenceType|phastCons|phylop"
            + "|populationFrequency|cDnaPosition|cdsPosition|proteinPosition|sift|polyphen|clinvar|cosmic|gwas|drugInteraction";
    private final StudyConfiguration studyConfiguration;
    private final VariantSourceDBAdaptor sourceDBAdaptor;
    private final OutputStream outputStream;
    private final QueryOptions queryOptions;

    private DecimalFormat df3 = new DecimalFormat("#.###");
    private VariantContextWriter writer;
    private List<String> annotations;
    private int failedVariants;

    public VariantVcfExporter(StudyConfiguration studyConfiguration, VariantSourceDBAdaptor sourceDBAdaptor, OutputStream outputStream,
                              QueryOptions queryOptions) {
        this.studyConfiguration = studyConfiguration;
        this.sourceDBAdaptor = sourceDBAdaptor;
        this.outputStream = outputStream;

        this.queryOptions = queryOptions;
    }


//    static {
//        try {
//            cellbaseClient = new CellBaseClient("bioinfo.hpc.cam.ac.uk", 80, "/cellbase/webservices/rest/", "v3", "hsapiens");
//        } catch (URISyntaxException e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * Uses a reader and a writer to dump a vcf.
     * TODO jmmut: use studyConfiguration to know the order of
     *
     * @param adaptor The query adaptor to execute the query
     * @param studyConfiguration Configuration object
     * @param outputUri The destination file
     * @param query The query object
     * @param options The options
     */
    public static void vcfExport(VariantDBAdaptor adaptor, StudyConfiguration studyConfiguration, URI outputUri, Query query,
                                 QueryOptions options) {

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
        List<Variant> variants = reader.read(batchSize);
//        while (!(variants = reader.read(batchSize)).isEmpty()) {
//            writer.write(variants);
//        }
        while (!variants.isEmpty()) {
            writer.write(variants);
            variants = reader.read(batchSize);
        }


        // tear down
        reader.post();
        reader.close();
        writer.post();
        writer.close();
    }

    public static int htsExport(VariantDBIterator iterator, StudyConfiguration studyConfiguration, VariantSourceDBAdaptor sourceDBAdaptor,
                                OutputStream outputStream, QueryOptions queryOptions) {

        VariantVcfExporter exporter = new VariantVcfExporter(studyConfiguration, sourceDBAdaptor, outputStream, queryOptions);

        exporter.open();
        exporter.pre();

        iterator.forEachRemaining(exporter::write);

        exporter.post();
        exporter.close();
        return exporter.failedVariants;
    }

    public boolean pre() {
        final VCFHeader header;
        try {
            header = getVcfHeader(studyConfiguration, queryOptions);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        header.addMetaDataLine(new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype"));
        header.addMetaDataLine(new VCFFormatHeaderLine("PF", VCFHeaderLineCount.A, VCFHeaderLineType.Integer,
                "variant was PASS filter in original sample gvcf"));
        header.addMetaDataLine(new VCFFilterHeaderLine("PASS", "Valid variant"));
        header.addMetaDataLine(new VCFFilterHeaderLine(".", "No FILTER info"));

        int studyId = studyConfiguration.getStudyId();
        //TODO: Need to prefix with the studyID ? Exporter is single study
        header.addMetaDataLine(new VCFInfoHeaderLine(studyId + "_PR", 1, VCFHeaderLineType.Float, "Pass rate"));
        header.addMetaDataLine(new VCFInfoHeaderLine(studyId + "_CR", 1, VCFHeaderLineType.Float, "Call rate"));
        header.addMetaDataLine(new VCFInfoHeaderLine(studyId + "_OPR", 1, VCFHeaderLineType.Float, "Overall Pass rate"));
        for (String cohortName : studyConfiguration.getCohortIds().keySet()) {
            if (cohortName.equals(StudyEntry.DEFAULT_COHORT)) {
                header.addMetaDataLine(new VCFInfoHeaderLine(VCFConstants.ALLELE_COUNT_KEY, VCFHeaderLineCount.A,
                        VCFHeaderLineType.Integer, "Total number of alternate alleles in called genotypes,"
                        + " for each ALT allele, in the same order as listed"));
                header.addMetaDataLine(new VCFInfoHeaderLine(VCFConstants.ALLELE_FREQUENCY_KEY, VCFHeaderLineCount.A,
                        VCFHeaderLineType.Float, "Allele Frequency, for each ALT allele, calculated from AC and AN, in the range (0,1),"
                        + " in the same order as listed"));
                header.addMetaDataLine(new VCFInfoHeaderLine(VCFConstants.ALLELE_NUMBER_KEY, 1,
                        VCFHeaderLineType.Integer, "Total number of alleles in called genotypes"));
                continue;
            }
//            header.addMetaDataLine(new VCFInfoHeaderLine(cohortName + VCFConstants.ALLELE_COUNT_KEY, VCFHeaderLineCount.A,
//                    VCFHeaderLineType.Integer, "Total number of alternate alleles in called genotypes,"
//                    + " for each ALT allele, in the same order as listed"));
            header.addMetaDataLine(new VCFInfoHeaderLine(cohortName + "_" + VCFConstants.ALLELE_FREQUENCY_KEY, VCFHeaderLineCount.A,
                    VCFHeaderLineType.Float,
                    "Allele frequency in the " + cohortName + " cohort calculated from AC and AN, in the range (0,1),"
                            + " in the same order as listed"));
//            header.addMetaDataLine(new VCFInfoHeaderLine(cohortName + VCFConstants.ALLELE_NUMBER_KEY, 1, VCFHeaderLineType.Integer,
//                    "Total number of alleles in called genotypes"));
        }

        // check if variant annotations are exported in the INFO column
        annotations = null;
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
            header.addMetaDataLine(new VCFInfoHeaderLine("CSQ", 1, VCFHeaderLineType.String, "Consequence annotations from CellBase. "
                    + "Format: " + annotationString));
        }

        final SAMSequenceDictionary sequenceDictionary = header.getSequenceDictionary();

        // setup writer
        VariantContextWriterBuilder builder = new VariantContextWriterBuilder()
                .setOutputStream(outputStream)
                .setReferenceDictionary(sequenceDictionary)
                .unsetOption(Options.INDEX_ON_THE_FLY);
        if (header.getSampleNamesInOrder().isEmpty()) {
            builder.setOption(Options.DO_NOT_WRITE_GENOTYPES);
        }
        writer = builder.build();

        writer.writeHeader(header);

        return true;
    }


    @Override
    public boolean write(List<Variant> batch) {
        for (Variant variant : batch) {
            try {
                VariantContext variantContext = convertVariantToVariantContext(variant, studyConfiguration, annotations);
                if (variantContext != null) {
                    writer.add(variantContext);
                }
            } catch (RuntimeException e) {
                e.printStackTrace(System.err);
                failedVariants++;
            }
        }
        return true;
    }


    @Override
    public boolean post() {
        if (failedVariants > 0) {
            logger.warn(failedVariants + " variants were not written due to errors");
        }
        return true;
    }

    @Override
    public boolean close() {
        writer.close();
        return true;
    }

    private VCFHeader getVcfHeader(StudyConfiguration studyConfiguration, QueryOptions options) throws IOException {
        //        get header from studyConfiguration
        Collection<String> headers = studyConfiguration.getHeaders().values();
        List<String> returnedSamples = null;
        if (options != null) {
            returnedSamples = options.getAsStringList(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key());
        }
        String fileHeader;
        if (headers.isEmpty()) {
            Iterator<VariantSource> iterator = sourceDBAdaptor.iterator(
                    new Query(VariantStorageManager.Options.STUDY_ID.key(), studyConfiguration.getStudyId()),
                    new QueryOptions());
            if (iterator.hasNext()) {
                VariantSource source = iterator.next();
                fileHeader = source.getMetadata().get(VariantFileUtils.VARIANT_FILE_HEADER).toString();
            } else {
                throw new IllegalStateException("file headers not available for study " + studyConfiguration.getStudyName()
                        + ". note: check files: " + studyConfiguration.getFileIds().values().toString());
            }
        } else {
            fileHeader = headers.iterator().next();
        }

        int lastLineIndex = fileHeader.lastIndexOf("#CHROM");
        if (lastLineIndex >= 0) {
            String substring = fileHeader.substring(0, lastLineIndex);
            if (returnedSamples == null) {
                BiMap<Integer, String> samplesPosition = StudyConfiguration.getIndexedSamplesPosition(studyConfiguration).inverse();
                returnedSamples = new ArrayList<>(samplesPosition.size());
                for (int i = 0; i < samplesPosition.size(); i++) {
                    returnedSamples.add(samplesPosition.get(i));
                }
            } else {
                List<String> newReturnedSamples = new ArrayList<>(returnedSamples.size());
                for (String returnedSample : returnedSamples) {
                    if (returnedSample.isEmpty()) {
                        continue;
                    } else if (StringUtils.isNumeric(returnedSample)) {
                        int sampleId = Integer.parseInt(returnedSample);
                        newReturnedSamples.add(studyConfiguration.getSampleIds().inverse().get(sampleId));
                    } else {
                        if (studyConfiguration.getSampleIds().containsKey(returnedSample)) {
                            newReturnedSamples.add(returnedSample);
                        } else {
                            throw new IllegalArgumentException("Unknown sample " + returnedSample + " for study "
                                    + studyConfiguration.getStudyName() + " (" + studyConfiguration.getStudyId() + ")");
                        }
                    }
                }
                returnedSamples = newReturnedSamples;
            }
            String samples = String.join("\t", returnedSamples);
            logger.debug("export will be done on samples: [{}]", samples);

            if (returnedSamples.isEmpty()) {
                fileHeader = substring + "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\t";
            } else {
                fileHeader = substring + "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + samples;
            }
        }

        return VariantFileMetadataToVCFHeaderConverter.parseVcfHeader(fileHeader);
    }

    /**
     * Convert org.opencb.biodata.models.variant.Variant into a htsjdk.variant.variantcontext.VariantContext
     * some assumptions:
     * * splitted multiallelic variants will produce only one variantContexts. Merging is done
     * * If some normalization has been applied, the source entries may have an attribute ORI like: "POS:REF:ALT_0(,ALT_N)*:ALT_IDX"
     *
     * @param variant A variant object to be converted
     * @param studyConfiguration StudyConfiguration
     * @param annotations Variant annotation
     * @return The variant in HTSJDK format
     */
    public VariantContext convertVariantToVariantContext(Variant variant, StudyConfiguration studyConfiguration,
                                                         List<String> annotations) { //, StudyConfiguration
        int studyId = studyConfiguration.getStudyId();
        VariantContextBuilder variantContextBuilder = new VariantContextBuilder();
        int start = variant.getStart();
        int end = variant.getEnd();
        String reference = variant.getReference();
        String alternate = variant.getAlternate();

        VariantType type = variant.getType();
        if (type == VariantType.INDEL) {
            reference = "N" + reference;
            alternate = "N" + alternate;
            start -= 1; // adjust start
        }

        String filter = "PASS";
        String prk = studyId + "_PR";
        String crk = studyId + "_CR";
        String oprk = studyId + "_OPR";

        //Attributes for INFO column
        ObjectMap attributes = new ObjectMap();

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

            attributes.putIfNotNull(prk, studyEntry.getAttributes().get("PR"));
            attributes.putIfNotNull(crk, studyEntry.getAttributes().get("CR"));
            attributes.putIfNotNull(oprk, studyEntry.getAttributes().get("OPR"));

            for (String sampleName : studyEntry.getOrderedSamplesName()) {
                String gtStr = studyEntry.getSampleData(sampleName, "GT");
                String genotypeFilter = studyEntry.getSampleData(sampleName, "FT");

                if (gtStr != null) {
                    List<String> gtSplit = new ArrayList<>(Arrays.asList(gtStr.split(",")));
                    List<String> ftSplit = new ArrayList<>(Arrays.asList(
                            (StringUtils.isBlank(genotypeFilter) ? "." : genotypeFilter).split(",")));
                    boolean filterIsMatching = gtSplit.size() == ftSplit.size();
                    String gt = gtSplit.get(0);
                    String ft = ftSplit.get(0);
                    if (gtSplit.size() > 1) {
//                        HashSet<String> set = new HashSet<>(gtSplit);
                        int idx = gtSplit.indexOf("0/0");
                        if (filterIsMatching) {
                            ftSplit.remove(idx);
                        }
                        gtSplit.remove(idx);
                        if (gtSplit.size() > 1) {
                            gt = ".";
                            ft = ".";
                        } else if (gtSplit.size() == 1) {
                            gt = gtSplit.get(0);
                            if (filterIsMatching) {
                                ft = ftSplit.get(0);
                            }
                        }
                    }
                    org.opencb.biodata.models.feature.Genotype genotype =
                            new org.opencb.biodata.models.feature.Genotype(gt, reference, alternate);
                    List<Allele> alleles = new ArrayList<>();
                    for (int gtIdx : genotype.getAllelesIdx()) {
                        if (gtIdx < originalAlleles.size() && gtIdx >= 0) {
                            alleles.add(Allele.create(originalAlleles.get(gtIdx), gtIdx == 0)); // allele is ref. if the alleleIndex is 0
                        } else {
                            alleles.add(Allele.create(".", false)); // genotype of a secondary alternate, or an actual missing
                        }
                    }

                    if (StringUtils.isBlank(ft)) {
                        genotypeFilter = null;
                    } else if (StringUtils.equals("PASS", ft)) {
                        genotypeFilter = "1";
                    } else {
                        genotypeFilter = "0";
                    }

                    GenotypeBuilder builder = new GenotypeBuilder()
                            .name(sampleName)
                            .alleles(alleles)
                            .phased(genotype.isPhased());
                    if (genotypeFilter != null) {
                        builder.attribute("PF", genotypeFilter);
                    }
                    genotypes.add(builder.make());
                }
            }

            addStats(studyEntry, attributes);
        }


        if (originalAlleles == null) {
            originalAlleles = allelesArray;
        }

        variantContextBuilder.start(originalPosition == null ? start : originalPosition)
                .stop((originalPosition == null ? start : originalPosition) + originalAlleles.get(0).length() - 1)
                .chr(variant.getChromosome())
                .filter(filter); // TODO jmmut: join attributes from different source entries? what to do on a collision?

        if (genotypes.isEmpty()) {
            variantContextBuilder.noGenotypes();
        } else {
            variantContextBuilder.genotypes(genotypes);
        }

        if (type.equals(VariantType.NO_VARIATION) && alternate.isEmpty()) {
            variantContextBuilder.alleles(reference);
        } else {
            variantContextBuilder.alleles(originalAlleles);
        }

        // if asked variant annotations are exported
        if (annotations != null) {
            addAnnotations(variant, annotations, attributes);
        }

        variantContextBuilder.attributes(attributes);


        if (StringUtils.isNotEmpty(variant.getId()) && !variant.toString().equals(variant.getId())) {
            StringBuilder ids = new StringBuilder();
            ids.append(variant.getId());
            if (variant.getNames() != null) {
                for (String name : variant.getNames()) {
                    ids.append(VCFConstants.ID_FIELD_SEPARATOR).append(name);
                }
            }
            variantContextBuilder.id(ids.toString());
        } else {
            variantContextBuilder.id(VCFConstants.EMPTY_ID_FIELD);
        }

        return variantContextBuilder.make();
    }

    private Map<String, Object> addAnnotations(Variant variant, List<String> annotations, Map<String, Object> attributes) {
        StringBuilder stringBuilder = new StringBuilder();
        if (variant.getAnnotation() == null) {
            return attributes;
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
                        if (consequenceType.getGeneName() != null) {
                            stringBuilder.append(consequenceType.getGeneName());
                        }
                        break;
                    case "ensemblGene":
                        if (consequenceType.getEnsemblGeneId() != null) {
                            stringBuilder.append(consequenceType.getEnsemblGeneId());
                        }
                        break;
                    case "ensemblTranscript":
                        if (consequenceType.getEnsemblTranscriptId() != null) {
                            stringBuilder.append(consequenceType.getEnsemblTranscriptId());
                        }
                        break;
                    case "biotype":
                        if (consequenceType.getBiotype() != null) {
                            stringBuilder.append(consequenceType.getBiotype());
                        }
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
                        if (consequenceType.getProteinVariantAnnotation() != null) {
                            stringBuilder.append(consequenceType.getProteinVariantAnnotation().getPosition());
                        }
                        break;
                    case "sift":
                        if (consequenceType.getProteinVariantAnnotation() != null
                                && consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                            List<Double> sift = consequenceType.getProteinVariantAnnotation().getSubstitutionScores().stream()
                                    .filter(t -> t.getSource().equalsIgnoreCase("sift"))
                                    .map(Score::getScore)
                                    .collect(Collectors.toList());
                            if (sift.size() > 0) {
                                stringBuilder.append(df3.format(sift.get(0)));
                            }
                        }
                        break;
                    case "polyphen":
                        if (consequenceType.getProteinVariantAnnotation() != null
                                && consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                            List<Double> polyphen = consequenceType.getProteinVariantAnnotation().getSubstitutionScores().stream()
                                    .filter(t -> t.getSource().equalsIgnoreCase("polyphen"))
                                    .map(Score::getScore)
                                    .collect(Collectors.toList());
                            if (polyphen.size() > 0) {
                                stringBuilder.append(df3.format(polyphen.get(0)));
                            }
                        }
                        break;
                    case "clinvar":
                        if (variant.getAnnotation().getVariantTraitAssociation() != null
                                && variant.getAnnotation().getVariantTraitAssociation().getClinvar() != null) {
                            stringBuilder.append(variant.getAnnotation().getVariantTraitAssociation().getClinvar().stream()
                                    .map(ClinVar::getTraits).flatMap(Collection::stream)
                                    .collect(Collectors.joining(",")));
                        }
                        break;
                    case "cosmic":
                        if (variant.getAnnotation().getVariantTraitAssociation() != null
                                && variant.getAnnotation().getVariantTraitAssociation().getCosmic() != null) {
                            stringBuilder.append(variant.getAnnotation().getVariantTraitAssociation().getCosmic().stream()
                                    .map(Cosmic::getPrimarySite)
                                    .collect(Collectors.joining(",")));
                        }
                        break;
                    case "gwas":
                        if (variant.getAnnotation().getVariantTraitAssociation() != null
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

        attributes.put("CSQ", stringBuilder.toString());
//        infoAnnotations.put("CSQ", stringBuilder.toString().replaceAll("&|$", ""));
        return attributes;
    }

    private void addStats(StudyEntry studyEntry, Map<String, Object> attributes) {
        if (studyEntry.getStats() == null) {
            return;
        }
        for (Map.Entry<String, VariantStats> entry : studyEntry.getStats().entrySet()) {
            String cohortName = entry.getKey();
            VariantStats stats = entry.getValue();

            if (cohortName.equals(StudyEntry.DEFAULT_COHORT)) {
                cohortName = "";
                int an = stats.getAltAlleleCount() + stats.getRefAlleleCount();
                if (an >= 0) {
                    attributes.put(cohortName + VCFConstants.ALLELE_NUMBER_KEY, String.valueOf(an));
                }
                if (stats.getAltAlleleCount() >= 0) {
                    attributes.put(cohortName + VCFConstants.ALLELE_COUNT_KEY, String.valueOf(stats.getAltAlleleCount()));
                }
            } else {
                cohortName = cohortName + "_";
            }
            attributes.put(cohortName + VCFConstants.ALLELE_FREQUENCY_KEY, DECIMAL_FORMAT.format(stats.getAltAlleleFreq()));
        }
    }

    /**
     * Assumes that ori is in the form "POS:REF:ALT_0(,ALT_N)*:ALT_IDX".
     * ALT_N is the n-th allele if this is the n-th variant resultant of a multiallelic vcf row
     *
     * @param ori
     * @return
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
     * Assumes that ori is in the form "POS:REF:ALT_0(,ALT_N)*:ALT_IDX".
     *
     * @param ori
     * @return
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


    /**
     * Unclosable output stream.
     *
     * Avoid passing System.out directly to HTSJDK, because it will close it at the end.
     *
     * http://stackoverflow.com/questions/8941298/system-out-closed-can-i-reopen-it/23791138#23791138
     */
    public static class UnclosableOutputStream extends FilterOutputStream {

        public UnclosableOutputStream(OutputStream os) {
            super(os);
        }

        @Override
        public void close() throws IOException {
            super.flush();
        }
    }
}

