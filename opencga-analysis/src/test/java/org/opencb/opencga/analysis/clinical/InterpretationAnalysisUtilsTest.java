package org.opencb.opencga.analysis.clinical;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.core.Gene;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.GeneClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.*;

public class InterpretationAnalysisUtilsTest {

    String dataFolder = "~/data150/";

//    @Test
    public void preFindings() throws IOException {
        String findingsInFilename = dataFolder + "/RR-extract_variant_2ary_findings_19_9_18.full.txt";
        String findingsOutFilename = "/tmp/actionableVariants_grch37.txt";

        String dbSnp;

        PrintWriter pw = new PrintWriter(Paths.get(findingsOutFilename).toFile());
        pw.println("#Chromosome\tStart\tEnd\tReference allele\tAlternate allele\tdbSNP ID\tClinVar variation ID\tHGVS\tPhenotype list\t"
                + "Clinical significance\tReview status\tSubmitter categories");

        File file = Paths.get(findingsInFilename).toFile();

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] split = line.split("\t");
            if ("-1".equals(split[3])) {
                dbSnp = ".";
            } else {
                dbSnp = "rs" + split[3];
            }
            pw.println(split[6] + "\t" + split[7] + "\t" + split[8] + "\t" + split[9] + "\t" + split[10] + "\t" + dbSnp
                    + "\t" + split[13] + "\t" + split[0] + "\t" + split[4] + "\t" + split[2] + "\t" + split[11] + "\t" + split[12]);
        }
        pw.close();
    }

//    @Test
    public void preRolesInCancer() throws IOException {
        String rolesInFilename = dataFolder + "/roleInCancer.txt";
        String rolesOutFilename = "/tmp/roleInCancer.txt";

        PrintWriter pw = new PrintWriter(Paths.get(rolesOutFilename).toFile());
        File file = Paths.get(rolesInFilename).toFile();
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        Map<String, String> geneNames = new HashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] split = line.split("\t");
            if (split.length > 1) {
                String[] roles = split[1].split(",");
                Set<String> rolesSet = new HashSet<>();
                for (String role : roles) {
                    switch (role) {
                        case "TSG":
                            rolesSet.add(ClinicalProperty.RoleInCancer.TUMOR_SUPPRESSOR_GENE.name());
                            break;
                        case "oncogene":
                            rolesSet.add(ClinicalProperty.RoleInCancer.ONCOGENE.name());
                            break;
                    }
                }
                if (rolesSet.size() == 1) {
                    geneNames.put(split[0], rolesSet.iterator().next());
                } else if (rolesSet.size() == 2) {
                    geneNames.put(split[0], ClinicalProperty.RoleInCancer.BOTH.name());
                }
            }
        }

        // CellBase client
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", "GRCh37", clientConfiguration);
        GeneClient geneClient = cellBaseClient.getGeneClient();

        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, "transcripts.exons,transcripts.cDnaSequence,annotation.expression");
        List<String> ids = new ArrayList<>(geneNames.keySet());
        QueryResponse<Gene> geneQueryResponse = geneClient.get(ids, options);
        for (QueryResult<Gene> result: geneQueryResponse.getResponse()) {
            for (Gene gene: result.getResult()) {
                if (geneNames.containsKey(gene.getName())) {
                    System.out.println(gene.getId() + ", " + gene.getName() + " -> " + geneNames.get(gene.getName()));
                    pw.println(gene.getId() + "\t" + geneNames.get(gene.getName()));
                }
            }
        }

        pw.close();
    }
}