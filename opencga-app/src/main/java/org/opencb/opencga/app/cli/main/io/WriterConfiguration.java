package org.opencb.opencga.app.cli.main.io;

/**
 * Created by pfurio on 28/11/16.
 */
public class WriterConfiguration {

    private boolean metadata;
    private boolean pretty;
    private boolean count;
    private boolean tree;

    public WriterConfiguration() {
    }

    public boolean isPretty() {
        return pretty;
    }

    public WriterConfiguration setPretty(boolean pretty) {
        this.pretty = pretty;
        return this;
    }

    public boolean isCount() {
        return count;
    }

    public WriterConfiguration setCount(boolean count) {
        this.count = count;
        return this;
    }

    public boolean isTree() {
        return tree;
    }

    public WriterConfiguration setTree(boolean tree) {
        this.tree = tree;
        return this;
    }

    public boolean isMetadata() {
        return metadata;
    }

    public WriterConfiguration setMetadata(boolean metadata) {
        this.metadata = metadata;
        return this;
    }
}
