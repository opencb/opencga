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

package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyQualityControl;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.nio.file.Path;
import java.util.Map;

public abstract class FamilyQcAnalysisExecutor extends OpenCgaToolExecutor {

    public enum QcType {
        RELATEDNESS
    }

    protected String studyId;
    protected Family family;
    protected String relatednessMethod;
    protected String relatednessMaf;
    protected Map<String, Map<String, Float>> relatednessThresholds;
    protected Path relatednessResourcePath;

    protected QcType qcType;

    protected FamilyQualityControl qualityControl;

    public FamilyQcAnalysisExecutor() {
    }

    public String getStudyId() {
        return studyId;
    }

    public FamilyQcAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public Family getFamily() {
        return family;
    }

    public FamilyQcAnalysisExecutor setFamily(Family family) {
        this.family = family;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public FamilyQcAnalysisExecutor setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public String getRelatednessMaf() {
        return relatednessMaf;
    }

    public FamilyQcAnalysisExecutor setRelatednessMaf(String relatednessMaf) {
        this.relatednessMaf = relatednessMaf;
        return this;
    }

    public Map<String, Map<String, Float>> getRelatednessThresholds() {
        return relatednessThresholds;
    }

    public FamilyQcAnalysisExecutor setRelatednessThresholds(Map<String, Map<String, Float>> relatednessThresholds) {
        this.relatednessThresholds = relatednessThresholds;
        return this;
    }

    public Path getRelatednesResourcePath() {
        return relatednessResourcePath;
    }

    public FamilyQcAnalysisExecutor setRelatednesResourcePath(Path relatednessResourcePath) {
        this.relatednessResourcePath = relatednessResourcePath;
        return this;
    }

    public QcType getQcType() {
        return qcType;
    }

    public FamilyQcAnalysisExecutor setQcType(QcType qcType) {
        this.qcType = qcType;
        return this;
    }

    public FamilyQualityControl getQualityControl() {
        return qualityControl;
    }

    public FamilyQcAnalysisExecutor setQualityControl(FamilyQualityControl qualityControl) {
        this.qualityControl = qualityControl;
        return this;
    }
}
