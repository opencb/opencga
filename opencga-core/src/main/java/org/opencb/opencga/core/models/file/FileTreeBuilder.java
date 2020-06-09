package org.opencb.opencga.core.models.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileTreeBuilder {

    private FileTreeBuilderNode fileTreeBuilderNode;

    public FileTreeBuilder(File baseDirectory) {
        this.fileTreeBuilderNode = new FileTreeBuilderNode(baseDirectory);
    }

    public void add(File file) {
        fileTreeBuilderNode.add(file);
    }

    public FileTree toFileTree() {
        return fileTreeBuilderNode.getFileTree();
    }

    private class FileTreeBuilderNode {

        private File file;
        private Map<String, FileTreeBuilderNode> fileTreeMap;

        public FileTreeBuilderNode(File file) {
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
                    fileTreeMap.put(tmpFile.getPath(), new FileTreeBuilderNode(tmpFile));
                }
                // Add the file in the correct nested level
                fileTreeMap.get(tmpFile.getPath()).add(newFile);
            } else {
                // File belongs in this level
                if (newFile.getType() == File.Type.FILE) {
                    fileTreeMap.put(filePath, new FileTreeBuilderNode(newFile));
                } else {
                    fileTreeMap.put(filePath + "/", new FileTreeBuilderNode(newFile));
                }
            }
        }

        public FileTree getFileTree() {
            FileTree fileTree = new FileTree(file);
            List<FileTree> children = new ArrayList<>(fileTreeMap.size());
            for (FileTreeBuilderNode value : fileTreeMap.values()) {
                children.add(value.getFileTree());
            }
            fileTree.setChildren(children);
            return fileTree;
        }
    }
}
