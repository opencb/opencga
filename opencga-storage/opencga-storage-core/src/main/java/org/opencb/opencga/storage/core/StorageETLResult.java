package org.opencb.opencga.storage.core;

import org.opencb.commons.datastore.core.ObjectMap;

import java.net.URI;

/**
 * Result of the indexation of a single file.
 *
 * Includes input and output files, timing, stats and possible exceptions.
 *
 * Created on 04/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StorageETLResult {

    protected URI input;

    protected URI extractResult;

    protected URI preTransformResult;
    protected URI transformResult;
    protected URI postTransformResult;

    protected URI preLoadResult;
    protected URI loadResult;
    protected URI postLoadResult;

    /**
     * Specifies if the transform step has been executed.
     */
    protected boolean transformExecuted;
    /**
     * Time in milliseconds of the transforming step.
     */
    protected long transformTimeMillis;
    /**
     * Transformation custom stats. Can be defined by the plugin.
     */
    protected ObjectMap transformStats;
    /**
     * Transform error, if any.
     */
    protected Exception transformError;

    /**
     * Specifies if the load step has been executed.
     */
    protected boolean loadExecuted;
    /**
     * Time in milliseconds of the loading step.
     */
    protected long loadTimeMillis;
    /**
     * Load custom stats. Can be defined by the plugin.
     */
    protected ObjectMap loadStats;
    /**
     * Load error, if any.
     */
    protected Exception loadError;

    public StorageETLResult() {
        this(null);
    }

    public StorageETLResult(URI input) {
        this.input = input;
        loadStats = new ObjectMap();
        loadExecuted = false;
        transformStats = new ObjectMap();
        transformExecuted = false;
    }

    @Override
    public String toString() {
        String transformStats;
        if (this.transformStats == null) {
            transformStats = "null";
        } else {
            try {
                transformStats = this.transformStats.toJson();
            } catch (RuntimeException e) {
                transformStats = this.transformStats.toString();
            }
        }
        String loadStats;
        if (this.loadStats == null) {
            loadStats = "null";
        } else {
            try {
                loadStats = this.loadStats.toJson();
            } catch (RuntimeException e) {
                loadStats = this.loadStats.toString();
            }
        }
        return "StorageETLResult{\n "
                + "\tinput : " + input + ",\n "
                + "\textractResult : " + extractResult + ",\n "
                + "\tpreTransformResult : " + preTransformResult + ",\n "
                + "\ttransformResult : " + transformResult + ",\n "
                + "\tpostTransformResult : " + postTransformResult + ",\n "
                + "\tpreLoadResult : " + preLoadResult + ",\n "
                + "\tloadResult : " + loadResult + ",\n "
                + "\tpostLoadResult : " + postLoadResult + ",\n "

                + "\ttransformExecuted : " + transformExecuted + ",\n "
                + "\ttransformError : " + transformError + ",\n "
                + "\ttransformTimeMillis : " + transformTimeMillis + ",\n "
                + "\ttransformStats : " + transformStats + ",\n "

                + "\tloadExecuted : " + loadExecuted + ",\n "
                + "\tloadError : " + loadError + ",\n "
                + "\tloadTimeMillis : " + loadTimeMillis + ",\n "
                + "\tloadStats : " + loadStats
                + "\n}";
    }

    public URI getInput() {
        return input;
    }

    public StorageETLResult setInput(URI input) {
        this.input = input;
        return this;
    }

    public URI getExtractResult() {
        return extractResult;
    }

    public StorageETLResult setExtractResult(URI extractResult) {
        this.extractResult = extractResult;
        return this;
    }

    public URI getPreTransformResult() {
        return preTransformResult;
    }

    public StorageETLResult setPreTransformResult(URI preTransformResult) {
        this.preTransformResult = preTransformResult;
        return this;
    }

    public URI getTransformResult() {
        return transformResult;
    }

    public StorageETLResult setTransformResult(URI transformResult) {
        this.transformResult = transformResult;
        return this;
    }

    public URI getPostTransformResult() {
        return postTransformResult;
    }

    public StorageETLResult setPostTransformResult(URI postTransformResult) {
        this.postTransformResult = postTransformResult;
        return this;
    }

    public URI getPreLoadResult() {
        return preLoadResult;
    }

    public StorageETLResult setPreLoadResult(URI preLoadResult) {
        this.preLoadResult = preLoadResult;
        return this;
    }

    public URI getLoadResult() {
        return loadResult;
    }

    public StorageETLResult setLoadResult(URI loadResult) {
        this.loadResult = loadResult;
        return this;
    }

    public URI getPostLoadResult() {
        return postLoadResult;
    }

    public StorageETLResult setPostLoadResult(URI postLoadResult) {
        this.postLoadResult = postLoadResult;
        return this;
    }

    public boolean isTransformExecuted() {
        return transformExecuted;
    }

    public StorageETLResult setTransformExecuted(boolean transformExecuted) {
        this.transformExecuted = transformExecuted;
        return this;
    }

    public Exception getTransformError() {
        return transformError;
    }

    public StorageETLResult setTransformError(Exception transformError) {
        this.transformError = transformError;
        return this;
    }

    public long getTransformTimeMillis() {
        return transformTimeMillis;
    }

    public StorageETLResult setTransformTimeMillis(long transformTimeMillis) {
        this.transformTimeMillis = transformTimeMillis;
        return this;
    }

    public ObjectMap getTransformStats() {
        return transformStats;
    }

    public StorageETLResult setTransformStats(ObjectMap transformStats) {
        this.transformStats = transformStats;
        return this;
    }

    public boolean isLoadExecuted() {
        return loadExecuted;
    }

    public StorageETLResult setLoadExecuted(boolean loadExecuted) {
        this.loadExecuted = loadExecuted;
        return this;
    }

    public Exception getLoadError() {
        return loadError;
    }

    public StorageETLResult setLoadError(Exception loadError) {
        this.loadError = loadError;
        return this;
    }

    public long getLoadTimeMillis() {
        return loadTimeMillis;
    }

    public StorageETLResult setLoadTimeMillis(long loadTimeMillis) {
        this.loadTimeMillis = loadTimeMillis;
        return this;
    }

    public ObjectMap getLoadStats() {
        return loadStats;
    }

    public StorageETLResult setLoadStats(ObjectMap loadStats) {
        this.loadStats = loadStats;
        return this;
    }
}
