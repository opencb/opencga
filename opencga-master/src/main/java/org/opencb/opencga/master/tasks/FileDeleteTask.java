package org.opencb.opencga.master.tasks;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.FileIndex;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.master.exceptions.TaskException;

import java.util.List;

public class FileDeleteTask extends OpenCgaTask {

    private String userToken;

    public enum Params {

        STUDY("study", "Study corresponding to the file to be deleted"),
        FILE("file", "File uuid, id or path corresponding to the file to be deleted"),
        USER("user", "User raising the deletion");

        private String id;
        private String description;

        Params(String id, String description) {
            this.id = id;
            this.description = description;
        }

        public String id() {
            return id;
        }

        public String description() {
            return description;
        }
    }

    public FileDeleteTask() {
        super();
    }

    @Override
    protected void check() throws TaskException {
        if (params == null || params.isEmpty()) {
            throw new TaskException("Missing parameters");
        }
        if (StringUtils.isEmpty(params.getString(Params.FILE.id()))) {
            throw new TaskException("Missing '" + Params.FILE.id() + "' parameter");
        }
        if (StringUtils.isEmpty(params.getString(Params.STUDY.id()))) {
            throw new TaskException("Missing '" + Params.STUDY.id() + "' parameter");
        }
        if (StringUtils.isEmpty(params.getString(Params.USER.id()))) {
            throw new TaskException("Missing '" + Params.USER.id() + "' parameter");
        }

        if (catalogManager == null) {
            throw new TaskException("Could not initialise CatalogManager.");
        }

        try {
            userToken = catalogManager.getUserManager().getSystemTokenForUser(params.getString(Params.USER.id()), opencgaToken);
        } catch (CatalogException e) {
            throw new TaskException(e.getCause());
        }
    }

    @Override
    protected void run() throws TaskException {
//        Study study = catalogManager.getStudyManager().resolveId(params.getString(Params.STUDY.id()), params.getString(Params.USER.id()));
//        OpenCGAResult<File> fileResult = catalogManager.getFileManager().get(study.getFqn(), params.getString(Params.FILE.id()),
//                FileManager.EXCLUDE_FILE_ATTRIBUTES, userToken);
//        if (fileResult.getNumResults() == 0) {
//            throw new TaskException("Could not find '" + params.get(Params.FILE.id()) + "' file");
//        }
//
//        File file = fileResult.first();
//
//        if (isIndexed(file)) {
//            // TODO: STEP 0. Remove file index
//
//        }
//
//        if (file.isExternal()) {
//            unlink(study, file);
//        } else {
//            delete(study, file);
//        }
    }

    @Override
    protected List<String> getSteps() {
        return null;
    }

    private boolean isIndexed(File file) {
        // Check the index status
        if (file.getIndex() != null && file.getIndex().getStatus() != null
                && !FileIndex.IndexStatus.NONE.equals(file.getIndex().getStatus().getName())
                && !FileIndex.IndexStatus.TRANSFORMED.equals(file.getIndex().getStatus().getName())) {
            return true;
        }

        return false;
    }

    private void unlink(Study study, File file) {

    }

    private void delete(Study study, File file) {

    }

}
