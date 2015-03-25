package org.opencb.opencga.storage.core.variant;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hpccoll1 on 23/03/15.
 */
public class FileStudyConfigurationManager extends StudyConfigurationManager {
    public static final String STUDY_CONFIGURATION_PATH = "studyConfigurationPath";
    protected static Logger logger = LoggerFactory.getLogger(FileStudyConfigurationManager.class);

    static final private Map<Integer, Path> filePaths = new HashMap<>();
    static final private Map<Integer, StudyConfiguration> studyConfigurationMap = new HashMap<>();

    public FileStudyConfigurationManager(ObjectMap objectMap) {
        super(objectMap);
    }

    @Override
    public QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, QueryOptions options) {
        long startTime = System.currentTimeMillis();

        StudyConfiguration studyConfiguration;
        if (studyConfigurationMap.containsKey(studyId)) {
            studyConfiguration = studyConfigurationMap.get(studyId);
        } else {
            Path path = getPath(studyId, options);
            try {
                studyConfiguration = read(path);
            } catch (IOException e) {
                logger.error("Fail at reading StudyConfiguration " + studyId, e);
                return new QueryResult<>(Integer.toString(studyId), (int) (System.currentTimeMillis() - startTime), 0, 0, "", e.getMessage(), Collections.<StudyConfiguration>emptyList());
            }
            studyConfigurationMap.put(studyId, studyConfiguration); //Add to the StudyConfiguration map
        }
        return new QueryResult<>(Integer.toString(studyId), (int) (System.currentTimeMillis() - startTime), 1, 1, "", "", Collections.singletonList(studyConfiguration));
    }

    @Override
    public QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long startTime = System.currentTimeMillis();

        Path path = getPath(studyConfiguration.getStudyId(), options);
        try {
            write(studyConfiguration, path);
            studyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration); //Update the StudyConfiguration map
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    private Path getPath(int studyId, QueryOptions options) {
        Path path;
        if (filePaths.containsKey(studyId)) {
            path = filePaths.get(studyId);
        } else {
            Object o = options.get(STUDY_CONFIGURATION_PATH);
            if (o == null) {
                //TODO: Read path from a default folder?
                return null;
            } else if (o instanceof Path) {
                path = (Path) o;
            } else {
                path = Paths.get(o.toString());
            }
            filePaths.put(studyId, path);
        }
        return path;
    }

    static public StudyConfiguration read(Path path) throws IOException {
        return new ObjectMapper(new JsonFactory()).readValue(path.toFile(), StudyConfiguration.class);
    }

    static public void write(StudyConfiguration studyConfiguration, Path path) throws IOException {
        new ObjectMapper(new JsonFactory()).writerWithDefaultPrettyPrinter().withoutAttribute("inverseFileIds").writeValue(path.toFile(), studyConfiguration);
    }


}
