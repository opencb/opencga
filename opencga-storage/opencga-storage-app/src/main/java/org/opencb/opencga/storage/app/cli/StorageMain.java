package org.opencb.opencga.storage.app.cli;

/**
 * Created by imedina on 02/03/15.
 */
public class StorageMain {

    public static void main(String[] args) {

        CliOptionsParser cliOptionsParser = new CliOptionsParser();
        cliOptionsParser.parse(args);

        String parsedCommand = cliOptionsParser.getCommand();
        if(parsedCommand == null || parsedCommand.isEmpty()) {
            if(cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
            }
            if(cliOptionsParser.getGeneralOptions().version) {
                System.out.println("version = 0.5.0");
            }
        }else {
            CommandExecutor commandExecutor = null;
            switch (parsedCommand) {
                case "index-variants":
                    if (cliOptionsParser.getIndexVariantsCommandOptions().help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new IndexVariantsCommandExecutor(cliOptionsParser.getIndexVariantsCommandOptions());
                    }
                    break;
                default:
                    break;
            }

            if (commandExecutor != null) {
//                try {
//                    commandParser.readCellBaseConfiguration();
                    commandExecutor.execute();
//                } catch (IOException |URISyntaxException ex) {
//                    commandParser.getLogger().error("Error reading cellbase configuration: " + ex.getMessage());
//                }
            }
        }
    }
}
