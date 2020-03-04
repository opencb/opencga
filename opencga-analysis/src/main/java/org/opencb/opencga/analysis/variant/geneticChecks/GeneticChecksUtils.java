package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.variant.MendelianErrorsReport;
import org.opencb.opencga.core.models.variant.MendelianErrorsReport.SampleAggregation;
import org.opencb.opencga.core.models.variant.MendelianErrorsReport.SampleAggregation.ChromosomeAggregation;
import org.opencb.opencga.core.models.variant.RelatednessReport;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.parboiled.common.StringUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.TPED;

public class GeneticChecksUtils {

    public static void selectMarkers(String basename, String study, List<String> samples, String population, Path outDir,
                                     VariantStorageManager storageManager, String token) throws ToolException {
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");

        // Split population format part1:part2, where:
        //   - part1 is the name of a study or the keyword "cohort"
        //   - part2 is the annotated population for that study or the cohort name
        // For annotated population studies, e.g.: 1kG_phase3:CEU
        // For cohort, e.g.: cohort:ALL
        String[] popSplits = population.split(":");

        // Apply filter: biallelic variants
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.TYPE.key(), "SNV");

        String gt = samples.stream().map(s -> s + ":0/0,0/1,1/1").collect(Collectors.joining(";"));
        query.put(VariantQueryParam.GENOTYPE.key(), gt);

        //.append(VariantQueryParam.FILTER.key(), "PASS")

        // Export variants in format .tped and .tfam to run PLINK
        // First, autosomal chromosomes
        File tpedAutosomeFile = outDir.resolve(basename + ".tped").toFile();
        File tfamAutosomeFile = outDir.resolve(basename + ".tfam").toFile();
        query.put(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22".split(",")));
        if (popSplits[0].equals("cohort")) {
            query.put(VariantQueryParam.STATS_MAF.key(), popSplits[1] + ">0.3");
        } else {
            query.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), population + ">0.3");
        }
        exportData(tpedAutosomeFile, tfamAutosomeFile, query, storageManager, token);
        if (tpedAutosomeFile.exists() && tpedAutosomeFile.length() > 0) {
            pruneVariants(basename, outputBinding);
        }

        // First, X chromosome
        File tpedXFile = outDir.resolve("x.tped").toFile();
        File tfamXFile = outDir.resolve("x.tfam").toFile();
        query.put(VariantQueryParam.REGION.key(), "X");
        if (popSplits[0].equals("cohort")) {
            query.put(VariantQueryParam.STATS_MAF.key(), popSplits[1] + ">0.05");
        } else {
            query.put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), population + ">0.05");
        }
        exportData(tpedXFile, tfamXFile, query, storageManager, token);
        if (tpedXFile.exists() && tpedXFile.length() > 0) {
            pruneVariants("x", outputBinding);
        }

        // Append files:
        //   - the x.tped file to autosome.tped file (since tfam files contain the same sample information)
        //   - the x.prune.out file to autosome.prune.out file
        appendFile(tpedXFile.getAbsolutePath(), tpedAutosomeFile.getAbsolutePath());
        appendFile(outDir.resolve("x.prune.out").toString(), outDir.resolve(basename + ".prune.out").toString());

        if (!tpedAutosomeFile.exists() || tpedAutosomeFile.length() == 0) {
            throw new ToolException("No variants found when exporting data to TPED/TFAM format");
        }
    }

    public static List<String> getSamples(String study, String familyId, CatalogManager catalogManager, String token)
            throws ToolException {
        Set<String> sampleSet = new HashSet<>();

        // Sanity check
        if (StringUtils.isEmpty(familyId)) {
            throw new ToolException("Missing family ID");
        }

        try {
            OpenCGAResult<Family> familyResult = catalogManager.getFamilyManager().get(study, familyId, QueryOptions.empty(), token);

            // Check family result
            if (familyResult.getResults().size() == 0) {
                throw new ToolException("Family not found for family ID '" + familyId + "'");
            }
            if (familyResult.getResults().size() > 1) {
                throw new ToolException("More than one family result for family ID '" + familyId + "'");
            }

            // Get list of individual IDs
            List<String> individualIds = familyResult.first().getMembers().stream().map(m -> m.getId()).collect(Collectors.toList());

            // Populate individual (and sample IDs) from individual IDs
            Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individualIds);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "samples.id");

            OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager().search(study, query, queryOptions, token);
            for (Individual individual : individualResult.getResults()) {
                sampleSet.addAll(individual.getSamples().stream().map(s -> s.getId()).collect(Collectors.toList()));
            }
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        return sampleSet.stream().collect(Collectors.toList());
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private static void exportData(File tpedFile, File tfamFile, Query query, VariantStorageManager storageManager,
                                   String token) throws ToolException {
        try {
            storageManager.exportData(tpedFile.getAbsolutePath(), TPED, null, query, QueryOptions.empty(), token);
        } catch(CatalogException | IOException | StorageEngineException e) {
            throw new ToolException(e);
        }

        if (!tpedFile.exists() || !tfamFile.exists()) {
            throw new ToolException("Something wrong exporting data to TPED/TFAM format");
        }
    }

    private static void pruneVariants(String basename, AbstractMap.SimpleEntry<String, String> outputBinding) throws ToolException {
        // Variant pruning using PLINK in docker
        String plinkParams = "plink --tfile /data/output/" + basename + " --indep 50 5 2 --out /data/output/" + basename;
        try {
            DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    private static void appendFile(String srcFilename, String destFilename) throws ToolException {
        if (new File(srcFilename).exists()) {
            try (FileWriter f = new FileWriter(destFilename, true);
                 PrintWriter p = new PrintWriter(new BufferedWriter(f))) {
                FileInputStream fis = new FileInputStream(srcFilename);
                Scanner sc = new Scanner(fis);
                while (sc.hasNextLine()) {
                    p.println(sc.nextLine());
                }
            } catch (IOException e) {
                throw new ToolException(e);
            }
        }
    }

    public static RelatednessReport buildRelatednessReport(String family, Path outDir) throws ToolException {
        RelatednessReport relatednessReport = new RelatednessReport();

        File file = outDir.resolve(family + ".genome").toFile();
        if (!file.exists()) {
            throw new ToolException("Missing relatedness file for family '" + family + "'");
        }

        // Set method
        relatednessReport.setMethod("IBD");

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file));
            // First line is the header
            // FID1         IID1 FID2         IID2 RT    EZ      Z0      Z1      Z2  PI_HAT PHE       DST     PPC   RATIO
            String line = reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] splits = line.split("\t");

                // Create relatedness score
                RelatednessReport.RelatednessScore score = new RelatednessReport.RelatednessScore();
                score.setSampleId1(splits[1]);
                score.setSampleId2(splits[3]);
                score.setZ0(Double.parseDouble(splits[6]));
                score.setZ1(Double.parseDouble(splits[7]));
                score.setZ2(Double.parseDouble(splits[8]));
                score.setPiHat(Double.parseDouble(splits[9]));
                score.setReportedRelation(splits[4]);

                // Add relatedness score to the report
                relatednessReport.getScores().add(score);
            }
            reader.close();
        } catch (IOException e) {
            throw new ToolException(e);
        }

        return relatednessReport;
    }

    public static MendelianErrorsReport buildMendelianErrorsReport(String family, Path outDir) throws ToolException {

        // Number of mendelian errors
        int numME;

        BufferedReader reader;

        // Family mendel file management: .fmendel file
        File file = outDir.resolve(family + ".fmendel").toFile();
        if (!file.exists()) {
            throw new ToolException("Missing family mendel file for family '" + family + "'");
        }

        try {
            reader = new BufferedReader(new FileReader(file));
            // First line is the header
            // FID           PAT           MAT   CHLD    N
            String line = reader.readLine();

            line = reader.readLine();
            if (line == null) {
                throw new ToolException("Error reading family mendel file for family '" + family + "'");
            }
            numME = Integer.parseInt(line.split("\t")[4]);
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Mendel file management: .mendel file
        file = outDir.resolve(family + ".mendel").toFile();
        if (!file.exists()) {
            throw new ToolException("Missing mendel file for family '" + family + "'");
        }

        // Map <sample, Map <chromosome, Map <code, in>>>
        Map<String, Map<String, Map<String, Integer>>> counter = new HashMap<>();

        try {
            reader = new BufferedReader(new FileReader(file));
            // First line is the header
            // FID   KID    CHR    SNP      CODE      ERROR
            String line = reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] splits = line.split("\t");

                String sample = splits[1];
                String chrom = splits[2];
                String errorCode = splits[4];

                // Check sample
                if (!counter.containsKey(sample)) {
                    counter.put(sample, new HashMap<>());
                }
                // Check chrom
                if (!counter.get(sample).containsKey(chrom)) {
                    counter.get(sample).put(chrom, new HashMap<>());
                }
                // Check error code
                if (!counter.get(sample).get(chrom).containsKey(errorCode)) {
                    counter.get(sample).get(chrom).put(errorCode, 0);
                }

                // Increment error counter
                counter.get(sample).get(chrom).put(errorCode, 1 + counter.get(sample).get(chrom).get(errorCode));
            }
        } catch (IOException e) {
            throw new ToolException(e);
        }


        // Update sample aggregation list from the previous maps
        List<SampleAggregation> sampleAggregationList = new ArrayList<>();
        int numVariants = getNumLines(outDir.resolve(family + ".lmendel").toFile());

        for (String sample : counter.keySet()) {
            int sampleErrors = 0;
            SampleAggregation sampleAggregation = new SampleAggregation();
            sampleAggregation.setSample(sample);

            for (String chrom : counter.get(sample).keySet()) {

                // Compute num. errors per chromosome
                int chromErrors = counter.get(sample).get(chrom).values().stream().reduce(0, Integer::sum);

                ChromosomeAggregation chromAggregation = new ChromosomeAggregation(chrom, chromErrors, counter.get(sample).get(chrom));

                // Unpdate num. errors and sample aggregation
                sampleErrors += chromErrors;
                sampleAggregation.getChromAggregation().add(chromAggregation);
            }

            // Set num. errors and ratio for that sample
            sampleAggregation.setNumErrors(sampleErrors);
            sampleAggregation.setRatio(1.0d * sampleErrors / numVariants);

            // Add sample aggregation to the mendelian error report
            sampleAggregationList.add(sampleAggregation);
        }

        return new MendelianErrorsReport(numME, sampleAggregationList);
    }

    private static int getNumLines(File file) throws ToolException {
        Command cmd = new Command(file.getAbsolutePath() + " -wl");
        cmd.run();
        String output = cmd.getOutput();
        if (StringUtils.isEmpty(output)) {
            throw new ToolException("Error reading number of lines: " + file.getAbsolutePath());
        }

        return (Integer.parseInt(output.split("\t")[0]) - 1);
    }
}
