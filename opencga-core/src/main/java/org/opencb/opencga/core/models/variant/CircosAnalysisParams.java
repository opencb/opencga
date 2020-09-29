/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;
import java.util.Map;

public class CircosAnalysisParams extends ToolParams {

    private String title;
    private Map<String, String> query;
    private List<CircosTrack> tracks;
    private Map<String, String> display;
    private String outdir;

    public static final String DESCRIPTION = "Circos analysis params to customize the plot. These parameters include the title, the "
            + "general query, the list of tracks and the display options. Supported track types are: SNV, INDEL, CNV, INSERTION, DELETION, "
            + "REARRANGEMENT, RAINPLOT, GENE, COVERAGE, COVERAGE_RATIO";

    public CircosAnalysisParams() {
    }

    public CircosAnalysisParams(String title, Map<String, String> query, List<CircosTrack> tracks, Map<String, String> display,
                                String outdir) {
        this.title = title;
        this.query = query;
        this.tracks = tracks;
        this.display = display;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CircosAnalysisParams{");
        sb.append("title='").append(title).append('\'');
        sb.append(", query=").append(query);
        sb.append(", tracks=").append(tracks);
        sb.append(", display=").append(display);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getTitle() {
        return title;
    }

    public CircosAnalysisParams setTitle(String title) {
        this.title = title;
        return this;
    }

    public Map<String, String> getQuery() {
        return query;
    }

    public CircosAnalysisParams setQuery(Map<String, String> query) {
        this.query = query;
        return this;
    }

    public List<CircosTrack> getTracks() {
        return tracks;
    }

    public CircosAnalysisParams setTracks(List<CircosTrack> tracks) {
        this.tracks = tracks;
        return this;
    }

    public Map<String, String> getDisplay() {
        return display;
    }

    public CircosAnalysisParams setDisplay(Map<String, String> display) {
        this.display = display;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public CircosAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
