package org.opencb.opencga.storage.hadoop.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.variant.search.pending.index.file.SecondaryIndexPendingVariantsFileBasedManager;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class PendingVariantsByFileMain extends AbstractMain {

    public static void main(String[] args) {
        PendingVariantsByFileMain main = new PendingVariantsByFileMain();
        try {
            main.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run(String[] args) throws Exception {
        new PendingVariantsByFileMain.CommandExecutor().exec(args);
    }

    public static class CommandExecutor extends NestedCommandExecutor {

        private final Configuration conf;

        public CommandExecutor() {
            conf = new HdfsConfiguration();
            addSubCommand(Arrays.asList("list", "ls"), "List pending variant files", args -> {
                SecondaryIndexPendingVariantsFileBasedManager manager = getManager("dummy");
                URI homeUri = manager.getHomeUri();

                FileSystem fs = FileSystem.get(homeUri, conf);
                if (!fs.exists(new Path(homeUri))) {
                    println("Home directory does not exist: " + homeUri);
                } else {
                    int count = 0;
                    for (FileStatus fileStatus : fs.listStatus(new Path(homeUri))) {
                        Path path = fileStatus.getPath();
                        Path pendingFolder = new Path(path.toUri().resolve(manager.getSubfolder() + "/"));
                        if (fs.exists(pendingFolder)) {
                            fileStatus = fs.getFileStatus(pendingFolder);
                            println(path.toUri() + " " + (fileStatus.isDirectory() ? "DIR" : "FILE") + " " + fileStatus.getLen());
                            count++;
                        }
                    }
                    if (count == 0) {
                        println("No pending variant directories found in " + homeUri);
                    }
                }
            });
            addSubCommand(Arrays.asList("info", "get"), "Print information about the pending variant files", args -> {
                URI pendingVariantsDir = URI.create(getArg(args, 0));
                ObjectMap argsMap = getArgsMap(args, 1);
                SecondaryIndexPendingVariantsFileBasedManager manager = getManager(pendingVariantsDir);

                if (manager.exists()) {
                    println("Pending variant directory: " + pendingVariantsDir);
                    int files = manager.checkFilesIntegrity();
                    if (files < 0) {
                        println("Corrupted variant files found in " + pendingVariantsDir);
                    } else {
                        println("Pending variant files found in " + pendingVariantsDir + ": " + files);
                    }
                    long pendingVariantsSize = manager.getPendingVariantsSize();
                    println("Total size of pending variant files: " + IOUtils.humanReadableByteCount(pendingVariantsSize, false)
                            + " (" + pendingVariantsSize + " bytes)");
                } else {
                    println("Pending variant directory does not exist: " + pendingVariantsDir);
                }
            });
            addSubCommand("list-files", "List files containing pending variants", args -> {
                URI pendingVariantsDir = URI.create(getArg(args, 0));
                SecondaryIndexPendingVariantsFileBasedManager manager = getManager(pendingVariantsDir);
                for (FileStatus file : manager.files()) {
                    println(file.getPath().toString() + " : " + IOUtils.humanReadableByteCount(file.getLen(), false)
                            + " (" + file.getLen() + " bytes)");
                }
            });
            addSubCommand("count", "Count pending variant", args -> {
                URI pendingVariantsDir = URI.create(getArg(args, 0));
                SecondaryIndexPendingVariantsFileBasedManager manager = getManager(pendingVariantsDir);
                ObjectMap argsMap = getArgsMap(args, 1, VariantQueryParam.REGION.key());
                try (VariantDBIterator iterator = manager.iterator(new Query(argsMap), 1000)) {
                    ProgressLogger progressLogger = new ProgressLogger("Counting pending variants").setBatchSize(100000);
                    long count = 0;
                    while (iterator.hasNext()) {
                        Variant variant = iterator.next();
                        progressLogger.increment(1, () -> "Up to variant " + variant);
                        count++;
                    }
                    println("Total pending variants in " + pendingVariantsDir + ": " + count);
                }
            });
            addSubCommand("list-variants", "List pending variants", args -> {
                URI pendingVariantsDir = URI.create(getArg(args, 0));
                SecondaryIndexPendingVariantsFileBasedManager manager = getManager(pendingVariantsDir);
                ObjectMap argsMap = getArgsMap(args, 1, VariantQueryParam.REGION.key());
                try (VariantDBIterator iterator = manager.iterator(new Query(argsMap), 1000)) {
                    while (iterator.hasNext()) {
                        Variant variant = iterator.next();
                        println(variant.toString());
                    }
                }
            });
        }

        private SecondaryIndexPendingVariantsFileBasedManager getManager(URI pendingVariantsDir) throws IOException {
            return new SecondaryIndexPendingVariantsFileBasedManager(pendingVariantsDir, conf);
        }
        private SecondaryIndexPendingVariantsFileBasedManager getManager(String variantsTable) throws IOException {
            return new SecondaryIndexPendingVariantsFileBasedManager(variantsTable, conf);
        }

    }

}
