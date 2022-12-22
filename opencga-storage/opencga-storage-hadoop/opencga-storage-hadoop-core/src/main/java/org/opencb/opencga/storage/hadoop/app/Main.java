package org.opencb.opencga.storage.hadoop.app;

import org.opencb.opencga.storage.hadoop.variant.migration.v2_4_13.ConvertIntoVirtual;

import java.util.Arrays;

public abstract class Main {

    public static void main(String[] mainArgs) throws Exception {
        AbstractMain.NestedCommandExecutor executor = new AbstractMain.NestedCommandExecutor();
        executor.addSubCommand(Arrays.asList("hbase", "hb"), "Run hbase utility commands", args -> {
            new HBaseMain().run(args);
        });
        executor.addSubCommand(Arrays.asList("metadata", "mm", "metadatamanager"), "Interact with HBase metadata manager",
                new VariantMetadataMain.VariantMetadataCommandExecutor());
        executor.addSubCommand(Arrays.asList("sample-index", "si"), "Debug options for scanning the SampleIndex", args -> {
            new SampleIndexMain().run(args);
        });
        executor.addSubCommand(Arrays.asList("pending-variants", "pending"), "Debug options for scanning the PendingVariants tables",
                args -> {
                    new PendingVariantsMain().run(args);
                });
        executor.addSubCommand(Arrays.asList("phoenix", "ph"), "Run phoenix utility commands", args -> {
            new PhoenixMain().run(args);
        });
        executor.addSubCommand(Arrays.asList("convertintovirtual"), "Migrate into virtual file", args -> {
            new ConvertIntoVirtual().run(args);
        });
        executor.exec(mainArgs);
    }

}
