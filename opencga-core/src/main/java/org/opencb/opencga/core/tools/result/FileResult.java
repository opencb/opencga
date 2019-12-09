package org.opencb.opencga.core.tools.result;

public class FileResult {
    public enum FileType {
        IMAGE,
        JSON,
        AVRO,
        PLAIN_TEXT,
        BINARY,
        TAB_SEPARATED // First line starts with # and contains the header
        ;

        public static FileType fromName(String fileName) {
            fileName = fileName.toLowerCase();
            if (fileName.endsWith(".gz")) {
                fileName = fileName.replace(".gz", "");
            }
            switch (fileName.substring(fileName.lastIndexOf('.') + 1)) {
                case "json":
                    return JSON;
                case "avro":
                    return AVRO;
                case "jpg":
                case "png":
                case "bmp":
                    return IMAGE;
                case "tsv":
                case "vcf":
                    return TAB_SEPARATED;
                case "txt":
                case "log":
                    return PLAIN_TEXT;
                default:
                    return BINARY;
            }
        }
    }

    private String path;
    private FileType type;

    public FileResult() {
    }

    public FileResult(String path, FileType type) {
        this.path = path;
        this.type = type;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("File{");
        sb.append("path=").append(path);
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public String getPath() {
        return path;
    }

    public FileResult setPath(String path) {
        this.path = path;
        return this;
    }

    public FileType getType() {
        return type;
    }

    public FileResult setType(FileType type) {
        this.type = type;
        return this;
    }
}
