/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public class StoragePipelineResult {

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

    public StoragePipelineResult() {
        this(null);
    }

    public StoragePipelineResult(URI input) {
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
        return "StoragePipelineResult{\n "
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

    public StoragePipelineResult setInput(URI input) {
        this.input = input;
        return this;
    }

    public URI getExtractResult() {
        return extractResult;
    }

    public StoragePipelineResult setExtractResult(URI extractResult) {
        this.extractResult = extractResult;
        return this;
    }

    public URI getPreTransformResult() {
        return preTransformResult;
    }

    public StoragePipelineResult setPreTransformResult(URI preTransformResult) {
        this.preTransformResult = preTransformResult;
        return this;
    }

    public URI getTransformResult() {
        return transformResult;
    }

    public StoragePipelineResult setTransformResult(URI transformResult) {
        this.transformResult = transformResult;
        return this;
    }

    public URI getPostTransformResult() {
        return postTransformResult;
    }

    public StoragePipelineResult setPostTransformResult(URI postTransformResult) {
        this.postTransformResult = postTransformResult;
        return this;
    }

    public URI getPreLoadResult() {
        return preLoadResult;
    }

    public StoragePipelineResult setPreLoadResult(URI preLoadResult) {
        this.preLoadResult = preLoadResult;
        return this;
    }

    public URI getLoadResult() {
        return loadResult;
    }

    public StoragePipelineResult setLoadResult(URI loadResult) {
        this.loadResult = loadResult;
        return this;
    }

    public URI getPostLoadResult() {
        return postLoadResult;
    }

    public StoragePipelineResult setPostLoadResult(URI postLoadResult) {
        this.postLoadResult = postLoadResult;
        return this;
    }

    public boolean isTransformExecuted() {
        return transformExecuted;
    }

    public StoragePipelineResult setTransformExecuted(boolean transformExecuted) {
        this.transformExecuted = transformExecuted;
        return this;
    }

    public Exception getTransformError() {
        return transformError;
    }

    public StoragePipelineResult setTransformError(Exception transformError) {
        this.transformError = transformError;
        return this;
    }

    public long getTransformTimeMillis() {
        return transformTimeMillis;
    }

    public StoragePipelineResult setTransformTimeMillis(long transformTimeMillis) {
        this.transformTimeMillis = transformTimeMillis;
        return this;
    }

    public ObjectMap getTransformStats() {
        return transformStats;
    }

    public StoragePipelineResult setTransformStats(ObjectMap transformStats) {
        this.transformStats = transformStats;
        return this;
    }

    public boolean isLoadExecuted() {
        return loadExecuted;
    }

    public StoragePipelineResult setLoadExecuted(boolean loadExecuted) {
        this.loadExecuted = loadExecuted;
        return this;
    }

    public Exception getLoadError() {
        return loadError;
    }

    public StoragePipelineResult setLoadError(Exception loadError) {
        this.loadError = loadError;
        return this;
    }

    public long getLoadTimeMillis() {
        return loadTimeMillis;
    }

    public StoragePipelineResult setLoadTimeMillis(long loadTimeMillis) {
        this.loadTimeMillis = loadTimeMillis;
        return this;
    }

    public ObjectMap getLoadStats() {
        return loadStats;
    }

    public StoragePipelineResult setLoadStats(ObjectMap loadStats) {
        this.loadStats = loadStats;
        return this;
    }
}
