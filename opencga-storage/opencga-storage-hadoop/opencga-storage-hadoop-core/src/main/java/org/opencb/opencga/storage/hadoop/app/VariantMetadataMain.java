package org.opencb.opencga.storage.hadoop.app;

import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    protected static final Logger LOGGER = LoggerFactory.getLogger(VariantMetadataMain.class);

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
        new VariantMetadataCommandExecutor().exec(args);
    }

    public static class VariantMetadataCommandExecutor extends NestedCommandExecutor {
        public VariantMetadataCommandExecutor() {
            this("metadata");
        }

        public VariantMetadataCommandExecutor(String argsContext) {
            super(argsContext);
            addSubCommand(Arrays.asList("tables", "table"), "[help|list]", new HBaseTablesCommandExecutor());
            addSubCommand(Arrays.asList("study-metadata", "sm", "study", "studies"), "[help|list|id|read|write] <metadata_table> ...",
                    new StudyCommandExecutor());
            addSubCommand(Arrays.asList("file-metadata", "fm", "file", "files"), "[help|list|id|read|write] <metadata_table> ...",
                    new FileCommandExecutor());
            addSubCommand(Arrays.asList("sample-metadata", "sample", "samples"), "[help|list|id|read|write] <metadata_table> ...",
                    new SampleCommandExecutor());
            addSubCommand(Arrays.asList("cohort-metadata", "cohort", "cohorts"), "[help|list|id|read|write] <metadata_table> ...",
                    new CohortCommandExecutor());
            addSubCommand(Arrays.asList("task-metadata", "task", "tasks"), "[help|list|id|read|write] <metadata_table> ...",
                    new TaskCommandExecutor());
        }
    }

    private static class HBaseTablesCommandExecutor extends NestedCommandExecutor {
        private HBaseManager hBaseManager;

        HBaseTablesCommandExecutor() {
            addSubCommand("list", "", args -> {
                List<String> tables = new LinkedList<>();
                for (TableName tableName : hBaseManager.getConnection().getAdmin().listTableNames()) {
                    if (HBaseVariantTableNameGenerator.isValidMetaTableName(tableName.getNameWithNamespaceInclAsString())) {
                        tables.add(tableName.getNameWithNamespaceInclAsString());
                    }
                }
                print(tables);
            });
        }

        @Override
        protected void setup(String command, String[] args) throws Exception {
            hBaseManager = new HBaseManager(HBaseConfiguration.create());
        }

        @Override
        protected void cleanup(String command, String[] args) throws Exception {
            hBaseManager.close();
        }
    }

    private static class VariantStorageMetadataManagerCommandExecutor extends NestedCommandExecutor {
        protected VariantStorageMetadataManager mm;

        @Override
        protected void setup(String command, String[] args) throws Exception {
            Configuration configuration = HBaseConfiguration.create();
            HBaseManager hBaseManager = new HBaseManager(configuration);
            String metaTableName = getArg(args, 0);
            mm = new VariantStorageMetadataManager(
                    new HBaseVariantStorageMetadataDBAdaptorFactory(hBaseManager, metaTableName, configuration));
        }

        @Override
        protected void cleanup(String command, String[] args) throws Exception {
            mm.close();
        }
    }

    private static class StudyCommandExecutor extends VariantStorageMetadataManagerCommandExecutor {
        StudyCommandExecutor() {
            addSubCommand(Arrays.asList("list", "search"),
                    "<metadata_table>",
                    args -> {
                        print(mm.getStudies());
                    }
            );
            addSubCommand(Arrays.asList("read", "info"),
                    "<metadata_table> <study>",
                    args -> {
                        print(mm.getStudyMetadata(getArg(args, 1)));
                    }
            );
            addSubCommand(Arrays.asList("write", "update"),
                    "<metadata_table> <studyMetadata.json>",
                    args -> {
                        mm.unsecureUpdateStudyMetadata(readFile(getArg(args, 1), StudyMetadata.class));
                    }
            );
            addSubCommand(Arrays.asList("rename"),
                    "<metadata_table> <currentStudyName> <newStudyName>",
                    args -> {
                        rename(getArg(args, 1), getArg(args, 2));
                    }
            );
        }

        private void rename(String currentStudyName, String newStudyName) throws StorageEngineException {
            Integer studyIdOrNull = mm.getStudyIdOrNull(newStudyName);
            if (studyIdOrNull != null) {
                throw new IllegalStateException("New study name already exists!");
            }
            int studyId = mm.getStudyId(currentStudyName);
            mm.updateStudyMetadata(studyId, studyMetadata -> {
                studyMetadata.setName(newStudyName);
            });
        }
    }

    public static class FileCommandExecutor extends ResourceCommandExecutor<FileMetadata> {
        protected FileCommandExecutor() {
            super(FileMetadata.class);
            addSubCommand(Collections.singletonList("create-virtual-file"),
                    "<metadata_table> <study> " + new CreateVirtualFileParams().toCliHelp(), this::createVirtualFile);
            addSubCommand(Collections.singletonList("list-indexed"), "<metadata_table> <study> [--includePartial]", this::listIndexed);
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

        public class CreateVirtualFileParams extends ToolParams {
            protected String virtualFileName;
            protected List<String> files;
        }

        protected void createVirtualFile(String[] args) throws Exception {
            int studyId = mm.getStudyId(getArg(args, 1));
            CreateVirtualFileParams params = getArgsMap(args, 2, new CreateVirtualFileParams());
            mm.registerVirtualFile(studyId, params.virtualFileName);
            mm.associatePartialFiles(studyId, params.virtualFileName, params.files);
        }

        protected void listIndexed(String[] args) throws Exception {
            int studyId = mm.getStudyId(getArg(args, 1));
            ObjectMap map = getArgsMap(args, 2, "includePartial");
            mm.getIndexedFiles(studyId, map.getBoolean("includePartial", false));
        }
    }

    public static class CohortCommandExecutor extends ResourceCommandExecutor<CohortMetadata> {
        protected CohortCommandExecutor() {
            super(CohortMetadata.class);
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

    public static class SampleCommandExecutor extends ResourceCommandExecutor<SampleMetadata> {
        protected SampleCommandExecutor() {
            super(SampleMetadata.class);
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

    public static class TaskCommandExecutor extends ResourceCommandExecutor<TaskMetadata> {
        protected TaskCommandExecutor() {
            super(TaskMetadata.class);
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

    public abstract static class ResourceCommandExecutor<T> extends VariantStorageMetadataManagerCommandExecutor {
        protected final Class<T> clazz;

        protected ResourceCommandExecutor(Class<T> clazz) {
            this.clazz = clazz;

            addSubCommand(Arrays.asList("list", "query", "search"),
                    "<metadata_table> <study> [id=<ID_1>,<ID_2>,...] [name=<NAME_REGEX_PATTERN>]",
                    args -> {
                        int studyId = mm.getStudyId(getArg(args, 1));
                        Predicate<T> filter = getFilter(args);
                        print(Iterators.filter(list(studyId), filter::test));
                    }
            );
            addSubCommand(Arrays.asList("id"),
                    "<metadata_table> <study> <" + clazz.getSimpleName() + ">",
                    args -> {
                        int studyId = mm.getStudyId(getArg(args, 1));
                        print(id(studyId, getArg(args, 2)));
                    }
            );
            addSubCommand(Arrays.asList("read", "info", "get"),
                    "<metadata_table> <study> <" + clazz.getSimpleName() + ">",
                    args -> {
                        int studyId = mm.getStudyId(getArg(args, 1));
                        print(read(studyId, id(studyId, getArg(args, 2))));
                    }
            );
            addSubCommand(Arrays.asList("write"),
                    "<metadata_table> <study> <" + clazz.getSimpleName() + ".json>",
                    args -> {
                        int studyId = mm.getStudyId(getArg(args, 1));
                        write(studyId, readFile(getArg(args, 2), clazz));
                    }
            );
//            addSubCommand(args -> {
//                        int studyId = mm.getStudyId(getArg(args, 1));
//                        delete(studyId, id(studyId, getArg(args, 2)));
//                    },
//                    "<metadata_table> <study> <" + clazz.getSimpleName() + ">",
//                    "delete"
//            );
//            addSubCommand(args -> {
//                    studyId = mm.getStudyId(getArg(args, 1));
//                    String arg = getArg(args, 2);
//                    T value = read.apply(studyId, StringUtils.isNumeric(arg) ? Integer.valueOf(arg) : arg);
//                    updateValue(value, args);
//                    write.accept(studyId, value);
//                    break;
//                },
//                    "<metadata_table> <study> <" + clazz.getSimpleName() + ">",
//                    "update"
//            );
        }

        protected abstract Iterator<T> list(int studyId);
        protected abstract int id(int studyId, Object object);
        protected abstract T read(int studyId, int id);
        protected abstract void write(int studyId, T object) throws StorageEngineException;
//        protected abstract void delete(int studyId, int id) throws StorageEngineException;
//        protected abstract void updateValue(int studyId, ???) throws StorageEngineException;

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
}
