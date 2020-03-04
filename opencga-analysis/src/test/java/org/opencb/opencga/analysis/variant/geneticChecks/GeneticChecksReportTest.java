package org.opencb.opencga.analysis.variant.geneticChecks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class GeneticChecksReportTest {

    @Test
    public void test() throws JsonProcessingException {

        long geneticChecksVarsCounter = 0;
        Set<Variable> geneticChecksVars = new LinkedHashSet<>();

        // Sample ID
        geneticChecksVars.add(new Variable()
                .setName("sampleId")
                .setId("sampleId")
                .setType(Variable.VariableType.STRING)
                .setRank(geneticChecksVarsCounter++)
                .setDescription("Sample ID"));

        // Father ID
        geneticChecksVars.add(new Variable()
                .setName("fatherId")
                .setId("fatherId")
                .setType(Variable.VariableType.STRING)
                .setRank(geneticChecksVarsCounter++)
                .setDescription("Father ID"));

        // Mother ID
        geneticChecksVars.add(new Variable()
                .setName("motherId")
                .setId("motherId")
                .setType(Variable.VariableType.STRING)
                .setRank(geneticChecksVarsCounter++)
                .setDescription("Mother ID"));

        // Sibling IDs
        geneticChecksVars.add(new Variable()
                .setName("siblingIds")
                .setId("siblingIds")
                .setType(Variable.VariableType.STRING)
                .setMultiValue(true)
                .setRank(geneticChecksVarsCounter++)
                .setDescription("Sibling IDs"));

        //---------------------------------------------------------------------
        // Sex report
        //---------------------------------------------------------------------

        long sexReportVarsCounter = 0;
        Set<Variable> sexReportVars = new LinkedHashSet<>();

        // Reported sex
        sexReportVars.add(new Variable()
                .setName("reportedSex")
                .setId("reportedSex")
                .setType(Variable.VariableType.STRING)
                .setRank(sexReportVarsCounter++)
                .setDescription("Reported sex"));

        // Reported karyotypic sex
        sexReportVars.add(new Variable()
                .setName("reportedKaryotypicSex")
                .setId("reportedKaryotypicSex")
                .setType(Variable.VariableType.STRING)
                .setRank(sexReportVarsCounter++)
                .setDescription("Reported karyotypic sex"));

        // Ratio X
        sexReportVars.add(new Variable()
                .setName("ratioX")
                .setId("ratioX")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(sexReportVarsCounter++)
                .setDescription("Ratio: X-chromoxome / autosomic-chromosomes"));

        // Ratio Y
        sexReportVars.add(new Variable()
                .setName("ratioY")
                .setId("ratioY")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(sexReportVarsCounter++)
                .setDescription("Ratio: Y-chromoxome / autosomic-chromosomes"));

        // Inferred karyotypic sex
        sexReportVars.add(new Variable()
                .setName("inferredKaryotypicSex")
                .setId("inferredKaryotypicSex")
                .setType(Variable.VariableType.STRING)
                .setRank(sexReportVarsCounter++)
                .setDescription("Inferred karyotypic sex"));


        // Add SexReport to GenticChecksReport
        geneticChecksVars.add(new Variable()
                .setName("sexReport")
                .setId("sexReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(geneticChecksVarsCounter++)
                .setDescription("Sex report")
                .setVariableSet(sexReportVars));

        //---------------------------------------------------------------------
        // Relatedness report
        //---------------------------------------------------------------------

        long relatednessReportVarsCounter = 0;
        Set<Variable> relatednessReportVars = new LinkedHashSet<>();

        // Method
        relatednessReportVars.add(new Variable()
                .setName("method")
                .setId("method")
                .setType(Variable.VariableType.STRING)
                .setRank(relatednessReportVarsCounter++)
                .setDescription("Method"));

        // Scores
        long scoresVarsCounter = 0;
        Set<Variable> scoresVars = new LinkedHashSet<>();

        // Sample ID #1
        scoresVars.add(new Variable()
                .setName("sampleId1")
                .setId("sampleId1")
                .setType(Variable.VariableType.STRING)
                .setRank(scoresVarsCounter++)
                .setDescription("Sample ID #1"));

        // Sample ID #2
        scoresVars.add(new Variable()
                .setName("sampleId2")
                .setId("sampleId2")
                .setType(Variable.VariableType.STRING)
                .setRank(scoresVarsCounter++)
                .setDescription("Sample ID #2"));

        // Reported relation
        scoresVars.add(new Variable()
                .setName("reportedRelation")
                .setId("reportedRelation")
                .setType(Variable.VariableType.STRING)
                .setRank(scoresVarsCounter++)
                .setDescription("Reported relation"));

        // Z0
        scoresVars.add(new Variable()
                .setName("z0")
                .setId("z0")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(scoresVarsCounter++)
                .setDescription("Z0"));

        // Z1
        scoresVars.add(new Variable()
                .setName("z1")
                .setId("z1")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(scoresVarsCounter++)
                .setDescription("Z1"));

        // Z2
        scoresVars.add(new Variable()
                .setName("z2")
                .setId("z2")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(scoresVarsCounter++)
                .setDescription("Z2"));

        // PI-HAT
        scoresVars.add(new Variable()
                .setName("piHat")
                .setId("piHat")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(scoresVarsCounter++)
                .setDescription("PI-HAT"));

        // Add Scores to RelatednessReport
        relatednessReportVars.add(new Variable()
                .setName("scores")
                .setId("scores")
                .setType(Variable.VariableType.OBJECT)
                .setRank(relatednessReportVarsCounter++)
                .setDescription("Scores")
                .setVariableSet(scoresVars));

        // Add RelatednessReport to GenticChecksReport
        geneticChecksVars.add(new Variable()
                .setName("relatednessReport")
                .setId("relatednessReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(geneticChecksVarsCounter++)
                .setDescription("Relatedness report")
                .setVariableSet(relatednessReportVars));

        //---------------------------------------------------------------------
        // Mendelian errors report
        //---------------------------------------------------------------------

        long mendelianErrorsReportVarsCounter = 0;
        Set<Variable> mendelianErrorsReportVars = new LinkedHashSet<>();

        // Number of errors for that sample
        mendelianErrorsReportVars.add(new Variable()
                .setName("numErrors")
                .setId("numErrors")
                .setType(Variable.VariableType.INTEGER)
                .setRank(mendelianErrorsReportVarsCounter++)
                .setDescription("Number of errors"));

        // Error ratio
        mendelianErrorsReportVars.add(new Variable()
                .setName("errorRatio")
                .setId("errorRatio")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(mendelianErrorsReportVarsCounter++)
                .setDescription("Error ratio"));

        // Aggregation per chromosome and error code
        long chromAggregationVarsCounter = 0;
        Set<Variable> chromAggregationVars = new LinkedHashSet<>();
        chromAggregationVars.add(new Variable()
                .setName("chromosome")
                .setId("chromosome")
                .setType(Variable.VariableType.STRING)
                .setRank(chromAggregationVarsCounter++)
                .setDescription("Chromosome"));

        chromAggregationVars.add(new Variable()
                .setName("numErrors")
                .setId("numErrors")
                .setType(Variable.VariableType.STRING)
                .setRank(chromAggregationVarsCounter++)
                .setDescription("Number of errors"));

        // Aggregation per error code for that chromosome
        chromAggregationVars.add(new Variable()
                .setName("codeAggregation")
                .setId("codeAggregation")
                .setType(Variable.VariableType.MAP_INTEGER)
                .setRank(chromAggregationVarsCounter++)
                .setDescription("Aggregation per error code for that chromosome"));

        // Aggregation per chromosome
        mendelianErrorsReportVars.add(new Variable()
                .setName("chromAggregation")
                .setId("chromAggregation")
                .setType(Variable.VariableType.OBJECT)
                .setRank(mendelianErrorsReportVarsCounter++)
                .setDescription("Aggregation per chromosome")
                .setVariableSet(chromAggregationVars));


        // Add MendelianErrorsReport to GenticChecksReport
        geneticChecksVars.add(new Variable()
                .setName("mendelianErrorsReport")
                .setId("mendelianErrorsReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(geneticChecksVarsCounter++)
                .setDescription("Mendelian errors report")
                .setVariableSet(mendelianErrorsReportVars));


        // Variable set
        VariableSet geneticChecksVs = new VariableSet()
                .setId("opencga_genetic_checks")
                .setName("opencga_genetic_checks")
                .setDescription("OpenCGA genetic checks")
                .setEntities(Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE))
                .setVariables(geneticChecksVars);

        // Generate JSON
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(geneticChecksVs));
    }
}
