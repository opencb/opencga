package org.opencb.opencga.core.models.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileTreeBuilder {

    @DataField(description = ParamConstants.FILE_TREE_BUILDER_FILE_DESCRIPTION)
    private File file;
    @DataField(description = ParamConstants.FILE_TREE_BUILDER_FILE_TREE_MAP_DESCRIPTION)
    private Map<String, FileTreeBuilder> fileTreeMap;

    public FileTreeBuilder(File file) {
        this.file = file;
        this.fileTreeMap = new HashMap<>();
    }

    public void add(File newFile) {
        if (newFile.getPath().equals(file.getPath())) {
            // We need to replace the current file. This file comes completely updated
            this.file = newFile;
            return;
        }

        // We have received a nested file
        String subPath = newFile.getPath().replace(file.getPath(), "");
        String[] split = subPath.split("/");
        String filePath = file.getPath() + split[0];
        if (split.length > 1) {
            File tmpFile = new File().setPath(filePath + "/");

            if (!fileTreeMap.containsKey(tmpFile.getPath())) {
                // We need to create a temporal file first and then call to add
                fileTreeMap.put(tmpFile.getPath(), new FileTreeBuilder(tmpFile));
            }
            // Add the file in the correct nested level
            fileTreeMap.get(tmpFile.getPath()).add(newFile);
        } else {
            // File belongs in this level
            if (newFile.getType() == File.Type.FILE) {
                fileTreeMap.put(filePath, new FileTreeBuilder(newFile));
            } else {
                fileTreeMap.put(filePath + "/", new FileTreeBuilder(newFile));
            }
        }
    }

    public FileTree toFileTree() {
        FileTree fileTree = new FileTree(file);
        List<FileTree> children = new ArrayList<>(fileTreeMap.size());
        for (FileTreeBuilder value : fileTreeMap.values()) {
            children.add(value.toFileTree());
        }
        fileTree.setChildren(children);
        return fileTree;
    }

}
