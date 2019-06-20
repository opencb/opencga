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

package org.opencb.opencga.server.rest;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.models.AnnotationSet;

import java.util.Map;

/**
 * Created by pfurio on 12/05/17.
 */
@Deprecated
public class CommonModels {

    public static class AnnotationSetParams {
        public String name;
        public String variableSet;
        public Map<String, Object> annotations;
        public Map<String, Object> attributes;

        public AnnotationSet toAnnotationSet(String studyStr, StudyManager studyManager, String sessionId) throws CatalogException {
//            AbstractManager.MyResourceId resource = studyManager.getVariableSetId(this.variableSet, studyId, sessionId);
            return new AnnotationSet(name, variableSet, annotations, attributes);
        }
    }

}
