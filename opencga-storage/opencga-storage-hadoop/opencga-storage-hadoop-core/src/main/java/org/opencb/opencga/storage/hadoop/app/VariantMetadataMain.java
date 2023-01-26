package org.opencb.opencga.storage.hadoop.app;

import com.google.common.collect.Iterators;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
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
public class VariantMetadataMain extends AbstractMain {

    public static void main(String[] args) {
        try {
            new VariantMetadataMain().run(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
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
                                if (HBaseVariantTableNameGenerator.isValidMetaTableName(tableName.getNameWithNamespaceInclAsString())) {
                                    tables.add(tableName.getNameWithNamespaceInclAsString());
                                }
                            }
                            print(tables);
                        }
                        break;
                    case "help":
                    default:
                        println("Commands:");
                        println("  " + command + " list");
                        println("  " + command + " help");
                        break;
                }
                break;
            case "sm":
            case "study":
            case "studies":
            case "study-metadata":
                switch (subCommand) {
                    case "list":
                    case "search":
                        print(mm.getStudies());
                        break;
                    case "read":
                    case "info":
                        print(mm.getStudyMetadata(getArg(args, 3)));
                        break;
                    case "write":
                    case "update":
                        mm.unsecureUpdateStudyMetadata(readFile(getArg(args, 3), StudyMetadata.class));
                        break;
                    case "help":
                    default:
                        println("Commands:");
                        println("  study-metadata list <metadata_table>");
                        println("  study-metadata read <metadata_table> <study>");
                        println("  study-metadata write <metadata_table> <studyMetadata.json>");
                }
                break;
            case "fm":
            case "file":
            case "files":
            case "file-metadata":
                new FileCommandExecutor(mm).exec(args, command, subCommand);
                break;
            case "vfm":
            case "variant-file":
            case "variant-files":
            case "variant-file-metadata":
                new VariantFileCommandExecutor(mm).exec(args, command, subCommand);
                break;
            case "sample":
            case "samples":
            case "sample-metadata":
                new SampleCommandExecutor(mm).exec(args, command, subCommand);
                break;
            case "cohort":
            case "cohorts":
            case "cohort-metadata":
                new CohortCommandExecutor(mm).exec(args, command, subCommand);
                break;
            case "task":
            case "tasks":
            case "task-metadata":
                new TaskCommandExecutor(mm).exec(args, command, subCommand);
                break;
            default:
            case "help":
                println("Commands:");
                println("  help");
                println("  tables                 [help|list]");
                println("  study-metadata         [help|list|id|read|write] <metadata_table> ...");
                println("  file-metadata          [help|list|id|read|write] <metadata_table> ...");
                println("  variant-file-metadata  [help|list|id|read|write] <metadata_table> ...");
                println("  sample-metadata        [help|list|id|read|write] <metadata_table> ...");
                println("  cohort-metadata        [help|list|id|read|write] <metadata_table> ...");
                println("  task-metadata          [help|list|id|read|write] <metadata_table> ...");
                break;
        }
        if (mm != null) {
            mm.close();
        }
    }

    public class FileCommandExecutor extends StudyResourceCommandExecutor<FileMetadata> {
        protected FileCommandExecutor(VariantStorageMetadataManager mm) {
            super(mm, FileMetadata.class);
        }

        @Override
        protected Iterator<FileMetadata> list(int studyId) {
            return mm.fileMetadataIterator(studyId);
        }

        @Override
        protected int id(int studyId, Object object) {
            return mm.getFileIdOrFail(studyId, object);
        }

        @Override
        protected FileMetadata read(int studyId, int id) {
            return mm.getFileMetadata(studyId, id);
        }

        @Override
        protected void write(int studyId, FileMetadata file) {
            mm.unsecureUpdateFileMetadata(studyId, file);
        }

    }

    public class VariantFileCommandExecutor extends ResourceCommandExecutor<VariantFileMetadata> {
        protected VariantFileCommandExecutor(VariantStorageMetadataManager mm) {
            super(mm, VariantFileMetadata.class);
        }

        @Override
        protected Iterator<VariantFileMetadata> list(int studyId) {
            try {
                return mm.variantFileMetadataIterator(studyId, new QueryOptions());
            } catch (StorageEngineException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected int id(int studyId, Object object) {
            return mm.getFileIdOrFail(studyId, object);
        }

        @Override
        protected VariantFileMetadata read(int studyId, int id) {
            try {
                return mm.getVariantFileMetadata(studyId, id, new QueryOptions()).first();
            } catch (StorageEngineException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void write(int studyId, VariantFileMetadata file) throws StorageEngineException {
            mm.updateVariantFileMetadata(studyId, file);
        }
    }

    public class CohortCommandExecutor extends StudyResourceCommandExecutor<CohortMetadata> {
        protected CohortCommandExecutor(VariantStorageMetadataManager mm) {
            super(mm, CohortMetadata.class);
        }

        @Override
        protected Iterator<CohortMetadata> list(int studyId) {
            return mm.cohortIterator(studyId);
        }

        @Override
        protected int id(int studyId, Object id) {
            return mm.getCohortIdOrFail(studyId, id);
        }

        @Override
        protected CohortMetadata read(int studyId, int id) {
            return mm.getCohortMetadata(studyId, id);
        }

        @Override
        protected void write(int studyId, CohortMetadata object) {
            mm.unsecureUpdateCohortMetadata(studyId, object);
        }
    }

    public class SampleCommandExecutor extends StudyResourceCommandExecutor<SampleMetadata> {
        protected SampleCommandExecutor(VariantStorageMetadataManager mm) {
            super(mm, SampleMetadata.class);
        }

        @Override
        protected Iterator<SampleMetadata> list(int studyId) {
            return mm.sampleMetadataIterator(studyId);
        }

        @Override
        protected int id(int studyId, Object object) {
            return mm.getSampleIdOrFail(studyId, object);
        }

        @Override
        protected SampleMetadata read(int studyId, int id) {
            return mm.getSampleMetadata(studyId, id);
        }

        @Override
        protected void write(int studyId, SampleMetadata object) {
            mm.unsecureUpdateSampleMetadata(studyId, object);
        }
    }

    public class TaskCommandExecutor extends ResourceCommandExecutor<TaskMetadata> {
        protected TaskCommandExecutor(VariantStorageMetadataManager mm) {
            super(mm, TaskMetadata.class);
        }

        @Override
        protected Iterator<TaskMetadata> list(int studyId) {
            return mm.taskIterator(studyId);
        }

        @Override
        protected int id(int studyId, Object object) {
            return Integer.parseInt(object.toString());
        }

        @Override
        protected TaskMetadata read(int studyId, int id) {
            return mm.getTask(studyId, id);
        }

        @Override
        protected void write(int studyId, TaskMetadata object) throws StorageEngineException {
            mm.unsecureUpdateTask(studyId, object);
        }

        @Override
        protected Predicate<TaskMetadata> getFilter(String k, String v) throws NoSuchMethodException {
            switch (k) {
                case "currentStatus":
                case "status":
                    return t -> t.currentStatus().equals(TaskMetadata.Status.valueOf(v.toUpperCase()));
                case "type":
                    return t -> t.getType().toString().equals(v);
                case "name":
                case "operationName":
                    return t -> t.getOperationName().equals(v);
                case "file":
                case "files":
                case "fileIds":
                    Set<Integer> fileIds = toIntSet(v);
                    return t -> t.getFileIds().stream().anyMatch(fileIds::contains);
                default:
                    return getDefaultFilters(k, v);
            }
        }
    }

    public abstract class StudyResourceCommandExecutor<T extends StudyResourceMetadata<T>> extends ResourceCommandExecutor<T> {

        protected StudyResourceCommandExecutor(VariantStorageMetadataManager mm, Class<T> clazz) {
            super(mm, clazz);
        }

        @Override
        protected Predicate<T> getFilter(String k, String v) throws NoSuchMethodException {
            if (k.startsWith("status.")) {
                String statusKey = StringUtils.removeStart(k, "status.");
                TaskMetadata.Status expectedStatus = TaskMetadata.Status.valueOf(v.toUpperCase(Locale.ROOT));
                return (t) -> expectedStatus == t.getStatus(statusKey);
            }
            switch (k.toLowerCase(Locale.ROOT)) {
                case "id":
                    int expectedId = Integer.parseInt(v);
                    return (t) -> t.getId() == expectedId;
                case "studyid":
                case "study":
                    int expectedStudy = Integer.parseInt(v);
                    return (t) -> t.getStudyId() == expectedStudy;
                case "name":
                    return (t) -> t.getName().equals(v);
                default:
                    return super.getFilter(k, v);
            }

        }
    }

    public abstract class ResourceCommandExecutor<T> {

        protected final VariantStorageMetadataManager mm;
        protected final Class<T> clazz;

        protected ResourceCommandExecutor(VariantStorageMetadataManager mm, Class<T> clazz) {
            this.mm = mm;
            this.clazz = clazz;
        }

        protected abstract Iterator<T> list(int studyId);
        protected abstract int id(int studyId, Object object);
        protected abstract T read(int studyId, int id);
        protected abstract void write(int studyId, T object) throws StorageEngineException;
//        protected abstract void delete(int studyId, int id) throws StorageEngineException;

        protected final void exec(String[] args, String command, String subCommand) throws Exception {
            int studyId;
            switch (subCommand) {
                case "list":
                case "query":
                case "search":
                    studyId = mm.getStudyId(getArg(args, 3));
                    Predicate<T> filter = getFilter(args);
                    print(Iterators.filter(list(studyId), filter::test));
                    break;
                case "id": {
                    studyId = mm.getStudyId(getArg(args, 3));
                    print(id(studyId, getArg(args, 4)));
                    break;
                }
                case "read":
                case "info": {
                    studyId = mm.getStudyId(getArg(args, 3));
                    print(read(studyId, id(studyId, getArg(args, 4))));
                    break;
                }
                case "write":
                    studyId = mm.getStudyId(getArg(args, 3));
                    write(studyId, readFile(getArg(args, 4), clazz));
                    break;
//                case "delete": {
//                    studyId = mm.getStudyId(getArg(args, 3));
//                    int id = id(studyId, getArg(args, 4));
//                    delete(studyId, id);
//                }
//                    break;
//            case "update": {
//                studyId = mm.getStudyId(getArg(args, 3));
//                String arg = getArg(args, 4);
//                T value = read.apply(studyId, StringUtils.isNumeric(arg) ? Integer.valueOf(arg) : arg);
//                updateValue(value, args);
//                write.accept(studyId, value);
//                break;
//            }
                case "help":
                default:
                    println("Commands:");
                    println("  " + command + " list <metadata_table> <study> [id=<ID_1>,<ID_2>,...] [name=<NAME_REGEX_PATTERN>]");
                    println("  " + command + " id <metadata_table> <study> <" + command + ">");
                    println("  " + command + " read <metadata_table> <study> <" + command + ">");
                    println("  " + command + " write <metadata_table> <study> <" + command + ".json>");
//                    println("  " + command + " delete <metadata_table> <study> <" + command + ">");

            }
        }

        protected final Predicate<T> getFilter(String[] args) throws Exception {
            Predicate<T> filter = f -> true;
            for (int i = 4; i < args.length; i++) {
                String arg = args[i];
                String[] kv = arg.split("=");
                String k = kv[0];
                String v = kv[1];

                Predicate<T> thisFilter = getFilter(k, v);
                if (thisFilter == null) {
                    throw new IllegalArgumentException("Unknown filter '" + k + "'");
                } else {
                    filter = filter.and(thisFilter);
                }
            }
            return filter;
        }

        protected final Predicate<T> getDefaultFilters(String k, String v) throws NoSuchMethodException {
            switch (k.toLowerCase()) {
                case "name":
                    Pattern pattern = Pattern.compile(v);
                    Method getName = clazz.getMethod("getName");
                    return t -> {
                        try {
                            return pattern.asPredicate().test((String) getName.invoke(t));
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw error(e);
                        }
                    };
                case "id":
                    Set<Integer> set = toIntSet(v);
                    Method getId = clazz.getMethod("getId");
                    return t -> {
                        try {
                            Number invoke = (Number) getId.invoke(t);
                            return set.contains(invoke.intValue());
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException(e);
                        }
                    };
                default:
                    return null;
            }
        }

        protected Predicate<T> getFilter(String k, String v) throws NoSuchMethodException {
            return getDefaultFilters(k, v);
        }

        protected final Set<Integer> toIntSet(String v) {
            return Arrays.stream(v.split(",")).map(Integer::valueOf).collect(Collectors.toSet());
        }

    }

    protected static VariantStorageMetadataManager buildVariantStorageMetadataManager(Configuration configuration, String metaTableName) {
        return new VariantStorageMetadataManager(
                new HBaseVariantStorageMetadataDBAdaptorFactory(new HBaseManager(configuration), metaTableName, configuration));
    }

}
