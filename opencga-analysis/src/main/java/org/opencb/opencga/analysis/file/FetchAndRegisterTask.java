package org.opencb.opencga.analysis.file;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

@Tool(id = FetchAndRegisterTask.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION,
        description = "Download an external file and register it in OpenCGA.")
public class FetchAndRegisterTask extends OpenCgaTool {

    public final static String ID = "files-fetch";

    private String studyFqn;
    private String urlStr;
    private String path;

    private String fileName;

    public FetchAndRegisterTask setStudy(String study) {
        this.studyFqn = study;
        return this;
    }

    public FetchAndRegisterTask setUrl(String url) {
        this.urlStr = url;
        return this;
    }

    public FetchAndRegisterTask setPath(String path) {
        this.path = path;
        return this;
    }

    @Override
    protected void check() throws Exception {
        if (StringUtils.isEmpty(studyFqn)) {
            throw new ToolException("Missing mandatory study parameter");
        }
        if (StringUtils.isEmpty(urlStr)) {
            throw new ToolException("Missing mandatory url");
        }

        String[] split = urlStr.split("/");
        fileName = split[split.length - 1];

        try {
            String userId = catalogManager.getUserManager().getUserId(token);
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, userId);

            OpenCGAResult<File> parents = catalogManager.getFileManager().getParents(studyFqn, path, false, QueryOptions.empty(), token);
            if (parents.getNumResults() == 0) {
                throw new ToolException("No parent folders found for " + path);
            }

            if (parents.first().isExternal()) {
                throw new CatalogException("Parent path " + parents.first().getPath() + " is external. Cannot download to mounted folders");
            }

            // Check write permissions over the path
            catalogManager.getAuthorizationManager()
                    .checkFilePermission(study.getUid(), parents.first().getUid(), userId, FileAclEntry.FilePermissions.WRITE);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected void run() throws Exception {
        step("download", () -> {
            ReadableByteChannel readableByteChannel;
            FileOutputStream fileOutputStream;
            try {
                URL url = new URL(urlStr);
                readableByteChannel = Channels.newChannel(url.openStream());
                fileOutputStream = new FileOutputStream(getOutDir().resolve(fileName).toString());
            } catch (IOException e) {
                throw new CatalogException("URL issue: " + e.getMessage(), e);
            }

            try {
                fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            } catch (Exception e) {
                deleteTemporaryFile();
                throw new CatalogException("Could not download file " + fileName, e);
            }
        });

        step("register", () -> {
            // Move downloaded file and register
            try {
                moveFile(studyFqn, getOutDir().resolve(fileName), null, path, token);
            } catch (Exception e) {
                deleteTemporaryFile();
            }
        });
    }

    @Override
    public List<String> getSteps() {
        return Arrays.asList("download", "register");
    }

    @Override
    protected void onShutdown() {
        try {
            deleteTemporaryFile();
        } catch (CatalogIOException e) {
            throw new RuntimeException("Could not delete temporal file", e);
        }
    }

    private void deleteTemporaryFile() throws CatalogIOException {
        if (Files.exists(getOutDir().resolve(fileName))) {
            catalogManager.getCatalogIOManagerFactory().get(getOutDir().toUri()).deleteFile(getOutDir().resolve(fileName).toUri());
        }
    }
}
