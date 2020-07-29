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

import org.apache.commons.collections.CollectionUtils;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;
import java.util.Map;

public class CircosAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Circos analysis params to customize the plot. These parameters include the title,  the "
    + "plot density (i.e., the number of points to display), the general query and the list of tracks. Currently, the supported "
    + "track types are: COPY-NUMBER, INDEL, REARRANGEMENT and SNV. In addition, each track can contain a specific query";
    private String title;
    private String density; // Density plot: LOW, MEDIUM or HIGH
    private Map<String, String> query;
    private List<CircosTrack> tracks;

    private String outdir;

    public CircosAnalysisParams() {
        this(null, "LOW", null, null, null);
    }

    public CircosTrack getCircosTrackByType(String type) {
        // Sanity check
        if (tracks == null || CollectionUtils.isEmpty(tracks)) {
            return null;
        }

        for (CircosTrack track: tracks) {
            if (type.equals(track.getType())) {
                // Return the first track
                return track;
            }
        }
        return null;
    }

    public CircosAnalysisParams(String title, String density, Map<String, String> query, List<CircosTrack> tracks, String outdir) {
        this.title = title;
        this.density = density;
        this.query = query;
        this.tracks = tracks;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CircosAnalysisParams{");
        sb.append("title='").append(title).append('\'');
        sb.append(", density='").append(density).append('\'');
        sb.append(", query=").append(query);
        sb.append(", tracks=").append(tracks);
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

    public String getDensity() {
        return density;
    }

    public CircosAnalysisParams setDensity(String density) {
        this.density = density;
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

    public String getOutdir() {
        return outdir;
    }

    public CircosAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
