package org.opencb.opencga.storage.hadoop.app;

import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created on 12/03/18.
 *
 * java -classpath opencga-storage-hadoop-core-1.4.0-rc-dev-jar-with-dependencies.jar:conf/hadoop/
 *      org.opencb.opencga.storage.hadoop.app.AdminMain tables list
 *
 * java -classpath opencga-storage-hadoop-core-1.4.0-rc-dev-jar-with-dependencies.jar:conf/hadoop/
 *      org.opencb.opencga.storage.hadoop.app.AdminMain studies list
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AdminMain extends AbstractMain {

    public static void main(String[] args) {
        try {
            new AdminMain().run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        String command = getArg(args, 0, "help");
        String subCommand = getArg(args, 1, "help");
        final VariantStorageMetadataManager mm;
        if (!command.equals("help") && !subCommand.equals("help") && !command.contains("table")) {
            String metaTableName = getArg(args, 2);
            mm = buildVariantStorageMetadataManager(configuration, metaTableName);
        } else {
            mm = null;
        }

        switch (command) {
            case "tables":
            case "table":
                switch (subCommand) {
                    case "list":
                        try (HBaseManager hBaseManager = new HBaseManager(configuration)) {
                            List<String> tables = new LinkedList<>();
                            for (TableName tableName : hBaseManager.getConnection().getAdmin().listTableNames()) {
                                tables.add(tableName.getNameWithNamespaceInclAsString());
                            }
                            print(tables);
                        }
                        break;
                    case "help":
                    default:
                        System.out.println("Commands:");
                        System.out.println("  " + command + " list");
                        System.out.println("  " + command + " help");
                        break;
                }
                break;
            case "sm":
            case "study":
            case "studies":
            case "study-metadata":
                switch (subCommand) {
                    case "list":
                        print(mm.getStudies());
                        break;
                    case "read":
                        print(mm.getStudyMetadata(getArg(args, 3)));
                        break;
                    case "write":
                        mm.unsecureUpdateStudyMetadata(read(getArg(args, 3), StudyMetadata.class));
                        break;
                    case "help":
                    default:
                        System.out.println("Commands:");
                        System.out.println("  study-metadata list <metadata_table>");
                        System.out.println("  study-metadata read <metadata_table> <study>");
                        System.out.println("  study-metadata write <metadata_table> <studyMetadata.json>");
                }
                break;
            case "fm":
            case "file":
            case "files":
            case "file-metadata":
                exec(mm, args, command, subCommand,
                        studyId -> mm.fileMetadataIterator(studyId),
                        (studyId, fileObj) -> mm.getFileMetadata(studyId, fileObj),
                        (studyId, file) -> mm.unsecureUpdateFileMetadata(studyId, file),
                        FileMetadata.class);

                break;
            case "sample":
            case "samples":
            case "sample-metadata":
                exec(mm, args, command, subCommand,
                        studyId -> mm.sampleMetadataIterator(studyId),
                        (Integer studyId, Object sampleId) -> mm.getSampleMetadata(studyId, mm.getSampleIdOrFail(studyId, sampleId)),
                        (studyId, sample) -> mm.unsecureUpdateSampleMetadata(studyId, sample),
                        SampleMetadata.class);
                break;
            case "cohort":
            case "cohorts":
            case "cohort-metadata":
                exec(mm, args, command, subCommand,
                        studyId -> mm.cohortIterator(studyId),
                        (studyId, cohort) -> mm.getCohortMetadata(studyId, cohort),
                        (studyId, cohort1) -> mm.unsecureUpdateCohortMetadata(studyId, cohort1),
                        CohortMetadata.class);
                break;
            case "task":
            case "tasks":
            case "task-metadata":
                exec(mm, args, command, subCommand,
                        studyId -> {
                            assert mm != null;
                            return mm.taskIterator(studyId);
                        },
                        (studyId, o) -> mm.getTask(studyId, Integer.valueOf(o.toString())),
                        (studyId, task) -> mm.unsecureUpdateTask(studyId, task),
                        TaskMetadata.class);
                break;
            default:
            case "help":
                System.out.println("Commands:");
                System.out.println("  help");
                System.out.println("  tables          [help|list]");
                System.out.println("  study-metadata  [help|list|read|write] <metadata_table> ...");
                System.out.println("  file-metadata   [help|list|read|write] <metadata_table> ...");
                System.out.println("  sample-metadata [help|list|read|write] <metadata_table> ...");
                System.out.println("  cohort-metadata [help|list|read|write] <metadata_table> ...");
                System.out.println("  task-metadata   [help|list|read|write] <metadata_table> ...");
                break;
        }
        if (mm != null) {
            mm.close();
        }
    }


    interface BiConsumer<A, B> {
        void accept(A a, B b) throws Exception;
    }

    interface BiFunction<A, B, R> {
        R apply(A a, B b) throws Exception;
    }

    protected <T> void exec(VariantStorageMetadataManager mm,
                                   String[] args, String command, String subCommand,
                                   Function<Integer, Iterator<T>> list,
                                   BiFunction<Integer, Object, T> read,
                                   BiConsumer<Integer, T> write, Class<T> type) throws Exception {

        int studyId;
        switch (subCommand) {
            case "list":
                studyId = mm.getStudyId(getArg(args, 3));
                Predicate<T> filter = getFilter(args, type);
                print(Iterators.filter(list.apply(studyId), filter::test));
                break;
            case "read":
                studyId = mm.getStudyId(getArg(args, 3));
                print(read.apply(studyId, getArg(args, 4)));
                break;
            case "write":
                studyId = mm.getStudyId(getArg(args, 3));
                write.accept(studyId, read(getArg(args, 4), type));
                break;
            case "help":
            default:
                System.out.println("Commands:");
                System.out.println("  " + command + " list <metadata_table> <study> [id=<ID_1>,<ID_2>,...] [name=<NAME_REGEX_PATTERN>]");
                System.out.println("  " + command + " read <metadata_table> <study> <" + command + "-id>");
                System.out.println("  " + command + " write <metadata_table> <study> <" + command + ".json>");

        }
    }

    private <T> Predicate<T> getFilter(String[] args, Class<T> type) throws Exception {
        Predicate<T> filter = f -> true;
        for (int i = 4; i < args.length; i++) {
            String arg = args[i];
            String[] kv = arg.split("=");
            String k = kv[0];
            String v = kv[1];

            switch (k.toLowerCase()) {
                case "status":
                    if (type.equals(TaskMetadata.class)) {
                        filter = filter.and(t -> ((TaskMetadata) t).currentStatus().equals(TaskMetadata.Status.valueOf(v.toUpperCase())));
                    } else {
                        throw error("Unknown filter " + k);
                    }
                    break;
                case "name":
                    Pattern pattern = Pattern.compile(v);
                    Method getName = type.getMethod("getName");
                    filter = filter.and(t -> {
                        try {
                            return pattern.asPredicate().test((String) getName.invoke(t));
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw error(e);
                        }
                    });
                    break;
                case "id":
                    Set<Integer> set = Arrays.stream(v.split(",")).map(Integer::valueOf).collect(Collectors.toSet());
                    Method getId = type.getMethod("getId");
                    filter = filter.and(t -> {
                        try {
                            Number invoke = (Number) getId.invoke(t);
                            return set.contains(invoke.intValue());
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    break;
                default:
                    throw new IllegalArgumentException("Unknown filter " + k);
            }
        }
        return filter;
    }

    protected static VariantStorageMetadataManager buildVariantStorageMetadataManager(Configuration configuration, String metaTableName) {
        return new VariantStorageMetadataManager(
                new HBaseVariantStorageMetadataDBAdaptorFactory(new HBaseManager(configuration), metaTableName, configuration));
    }

}
