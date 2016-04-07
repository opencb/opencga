/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.alignment;

import org.bson.Document;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.commons.datastore.core.ComplexTypeConverter;

import java.util.List;

/**
 * Created by Jacobo Coll on 26/08/14.
 */
public class DocumentToMeanCoverageConverter implements ComplexTypeConverter<MeanCoverage, Document> {

    // <size>_<chromosome>_<chunk_number>

    public Document getIdObject(MeanCoverage meanCoverage) {
        return new Document(CoverageMongoDBWriter.ID_FIELD, getIdField(meanCoverage));
    }

    public String getIdField(MeanCoverage meanCoverage) {
        return String.format("%s_%d_%s", meanCoverage.getRegion().getChromosome(), meanCoverage.getRegion().getStart() / meanCoverage
                .getSize(), meanCoverage.getName().toLowerCase());
    }

    @Override
    public MeanCoverage convertToDataModelType(Document document) {
        String[] split = document.get(CoverageMongoDBWriter.ID_FIELD).toString().split("_");

        float averageFloat;
        Object average;
        if (document.containsKey(CoverageMongoDBWriter.FILES_FIELD)) {
            average = ((Document) ((List) document.get(CoverageMongoDBWriter.FILES_FIELD)).get(0)).get(CoverageMongoDBWriter
                    .AVERAGE_FIELD);
            //coverage = ((Double) ((Document) ((LinkedList<>) dbObject.get(CoverageMongoWriter.FILES_FIELD)).get(0)).get
            // (CoverageMongoWriter.AVERAGE_FIELD)).floatValue();
        } else if (document.containsKey(CoverageMongoDBWriter.AVERAGE_FIELD)) {
            average = document.get(CoverageMongoDBWriter.AVERAGE_FIELD);
            //coverage = ((Double) dbObject.get(CoverageMongoWriter.AVERAGE_FIELD)).floatValue();
        } else {
            //TODO: Show a error message
            return null;
        }
        if (average instanceof Float) {
            averageFloat = (Float) average;
        } else if (average instanceof Double) {
            averageFloat = ((Double) average).floatValue();
        } else if (average == null) {
            return null;
        } else {
            averageFloat = Float.parseFloat(average.toString());
        }
        return new MeanCoverage(split[2], split[0], Integer.parseInt(split[1]), averageFloat);

    }

    @Override
    public Document convertToStorageType(MeanCoverage meanCoverage) {
        return new Document(CoverageMongoDBWriter.AVERAGE_FIELD, meanCoverage.getCoverage());
    }
}
