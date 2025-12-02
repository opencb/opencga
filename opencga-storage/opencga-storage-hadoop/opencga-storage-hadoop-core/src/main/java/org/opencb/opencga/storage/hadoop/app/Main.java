package org.opencb.opencga.storage.hadoop.app;

import org.opencb.opencga.storage.hadoop.variant.migration.v2_4_13.ConvertIntoVirtual;

import java.util.Arrays;

public abstract class Main {

    public static void main(String[] mainArgs) throws Exception {
        AbstractMain.NestedCommandExecutor executor = new AbstractMain.NestedCommandExecutor();
        executor.addSubCommand(Arrays.asList("hbase", "hb"), "Run hbase utility commands",
                new HBaseMain());
        executor.addSubCommand(Arrays.asList("variant-engine", "variants", "ve", "variant"), "General variant engine utilities",
                new VariantEngineUtilsMain.VariantEngineUtilsCommandExecutor());
        executor.addSubCommand(Arrays.asList("metadata", "mm", "metadatamanager"), "Interact with HBase Variant metadata manager",
                new VariantMetadataMain.VariantMetadataCommandExecutor());
        executor.addSubCommand(Arrays.asList("sample-index", "si"), "Debug options for scanning the SampleIndex",
                new SampleIndexMain());
        executor.addSubCommand(Arrays.asList("pending-variants-table", "pending-variants", "pending", "pvt"),
                "Debug options for scanning the PendingVariants tables",
                new PendingVariantsMain());
        executor.addSubCommand(Arrays.asList("pending-variants-file", "pvf"),
                "Debug options for scanning the PendingVariants for secondary annotation index table",
                new PendingVariantsByFileMain.CommandExecutor());
        executor.addSubCommand(Arrays.asList("phoenix", "ph"), "Run phoenix utility commands",
                new PhoenixMain());
        executor.addSubCommand(Arrays.asList("convertintovirtual", "ConvertIntoVirtual"), "Migrate into virtual file",
            new ConvertIntoVirtual());
        executor.addSubCommand(Arrays.asList("hadoop", "hdfs"), "Run hadoop commands",
                new HadoopMain.HadoopCommandExecutor());
        executor.exec(mainArgs);
    }

}
