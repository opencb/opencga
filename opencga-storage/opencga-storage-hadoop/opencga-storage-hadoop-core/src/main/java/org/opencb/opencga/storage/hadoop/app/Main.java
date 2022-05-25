package org.opencb.opencga.storage.hadoop.app;

import java.util.Arrays;

public abstract class Main {

    public static void main(String[] args) throws Exception {
        String command;
        if (args.length == 0) {
            command = "help";
        } else {
            command = args[0];
            args = Arrays.copyOfRange(args, 1, args.length);
        }
        String inputCommand = command;
        command = command.toLowerCase().replace("-", "").replace("_", "");
        switch (command) {
            case "hb":
            case "hbase":
                new HBaseMain().run(args);
                break;
            case "mm":
            case "metadata":
            case "metadatamanager":
                new VariantMetadataMain().run(args);
                break;
            case "si":
            case "sampleindex":
                new SampleIndexMain().run(args);
                break;
            case "pending":
            case "pendingvariants":
            case "pv":
                new PendingVariantsMain().run(args);
                break;
            case "help":
                printHelp();
                break;
            default:
                System.err.println("Unknown command " + inputCommand);
                printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Commands:");
        System.out.println("  help             Print this help ");
        System.out.println("  hbase            Run hbase utility commands");
        System.out.println("  metadata         Interact with HBase metadata manager");
        System.out.println("  sample-index     Debug options for scanning the SampleIndex");
        System.out.println("  pending-variants Debug options for scanning the PendingVariants tables");
    }
}
