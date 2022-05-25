package org.opencb.opencga.app.cli.main.executors;

import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

import static org.opencb.commons.utils.PrintUtils.printError;

public class ExecutorProvider {

    public static OpencgaCommandExecutor getOpencgaCommandExecutor(OpencgaCliOptionsParser cliOptionsParser, String parsedCommand) throws CatalogAuthenticationException {
        OpencgaCommandExecutor commandExecutor = null;
        switch (parsedCommand) {
            case "users":
                commandExecutor = new UsersCommandExecutor(cliOptionsParser.getUsersCommandOptions());
                break;
            case "projects":
                commandExecutor = new ProjectsCommandExecutor(cliOptionsParser.getProjectsCommandOptions());
                break;
            case "studies":
                commandExecutor = new StudiesCommandExecutor(cliOptionsParser.getStudiesCommandOptions());
                break;
            case "files":
                commandExecutor = new FilesCommandExecutor(cliOptionsParser.getFilesCommandOptions());
                break;
            case "jobs":
                commandExecutor = new JobsCommandExecutor(cliOptionsParser.getJobsCommandOptions());
                break;
            case "individuals":
                commandExecutor = new IndividualsCommandExecutor(cliOptionsParser.getIndividualsCommandOptions());
                break;
            case "samples":
                commandExecutor = new SamplesCommandExecutor(cliOptionsParser.getSamplesCommandOptions());
                break;
            case "cohorts":
                commandExecutor = new CohortsCommandExecutor(cliOptionsParser.getCohortsCommandOptions());
                break;
            case "panels":
                commandExecutor = new DiseasePanelsCommandExecutor(cliOptionsParser.getDiseasePanelsCommandOptions());
                break;
            case "families":
                commandExecutor = new FamiliesCommandExecutor(cliOptionsParser.getFamiliesCommandOptions());
                break;
            case "alignments":
                commandExecutor =
                        new AnalysisAlignmentCommandExecutor(cliOptionsParser.getAnalysisAlignmentCommandOptions());
                break;
            case "variant":
                commandExecutor =
                        new AnalysisVariantCommandExecutor(cliOptionsParser.getAnalysisVariantCommandOptions());
                break;
            case "clinical":
                commandExecutor =
                        new AnalysisClinicalCommandExecutor(cliOptionsParser.getAnalysisClinicalCommandOptions());
                break;
            case "operations":
                commandExecutor =
                        new OperationsVariantStorageCommandExecutor(cliOptionsParser.getOperationsVariantStorageCommandOptions());
                break;
            case "meta":
                commandExecutor = new MetaCommandExecutor(cliOptionsParser.getMetaCommandOptions());
                break;
            default:
                printError("Not valid command passed: '" + parsedCommand + "'");
                break;
        }
        return commandExecutor;
    }

}
