package org.opencb.opencga.core.tools.result;

import java.net.URI;

public class FileMove {
    private String source;
    private CatalogFile target;

    public String getSource() {
        return source;
    }

    public FileMove setSource(String source) {
        this.source = source;
        return this;
    }

    public CatalogFile getTarget() {
        return target;
    }

    public FileMove setTarget(CatalogFile target) {
        this.target = target;
        return this;
    }

    public static class CatalogFile {
        /** Catalog path */
        private String path;
        /** Target URI */
        private URI uri;

        public CatalogFile() {
        }

        public CatalogFile(String path, URI uri) {
            this.path = path;
            this.uri = uri;
        }

        public String getPath() {
            return path;
        }

        public CatalogFile setPath(String path) {
            this.path = path;
            return this;
        }

        public URI getUri() {
            return uri;
        }

        public CatalogFile setUri(URI uri) {
            this.uri = uri;
            return this;
        }
    }
}
