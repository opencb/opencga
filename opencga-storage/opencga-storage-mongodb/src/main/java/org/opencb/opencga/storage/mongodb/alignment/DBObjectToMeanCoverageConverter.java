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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 * Created by Jacobo Coll on 26/08/14.
 */
public class DBObjectToMeanCoverageConverter implements ComplexTypeConverter<MeanCoverage, DBObject> {

    // <size>_<chromosome>_<chunk_number>

    public DBObject getIdObject(MeanCoverage meanCoverage) {
        return new BasicDBObject(CoverageMongoDBWriter.ID_FIELD, getIdField(meanCoverage));
    }

    public String getIdField(MeanCoverage meanCoverage) {
        return String.format("%s_%d_%s", meanCoverage.getRegion().getChromosome(), meanCoverage.getRegion().getStart() / meanCoverage
                .getSize(), meanCoverage.getName().toLowerCase());
    }

    @Override
    public MeanCoverage convertToDataModelType(DBObject dbObject) {
        String[] split = dbObject.get(CoverageMongoDBWriter.ID_FIELD).toString().split("_");

        float averageFloat;
        Object average;
        if (dbObject.containsField(CoverageMongoDBWriter.FILES_FIELD)) {
            average = ((BasicDBObject) ((BasicDBList) dbObject.get(CoverageMongoDBWriter.FILES_FIELD)).get(0)).get(CoverageMongoDBWriter
                    .AVERAGE_FIELD);
            //coverage = ((Double) ((BasicDBObject) ((BasicDBList) dbObject.get(CoverageMongoWriter.FILES_FIELD)).get(0)).get
            // (CoverageMongoWriter.AVERAGE_FIELD)).floatValue();
        } else if (dbObject.containsField(CoverageMongoDBWriter.AVERAGE_FIELD)) {
            average = dbObject.get(CoverageMongoDBWriter.AVERAGE_FIELD);
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
    public DBObject convertToStorageType(MeanCoverage meanCoverage) {

        BasicDBObject mCoverage = new BasicDBObject(CoverageMongoDBWriter.AVERAGE_FIELD, meanCoverage.getCoverage());

        return mCoverage;
    }
}
