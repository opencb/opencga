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
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 * Created by jacobo on 26/08/14.
 */
public class DBObjectToRegionCoverageConverter implements ComplexTypeConverter<RegionCoverage, DBObject> {

    public DBObject getIdObject(RegionCoverage regionCoverage) {
        return new BasicDBObject(CoverageMongoDBWriter.ID_FIELD, getIdField(regionCoverage));
    }

    public String getIdField(RegionCoverage regionCoverage) {
        int size = regionCoverage.getAll().length;

        String chunkId;
        if (size % 1000000 == 0 && size / 1000000 != 0) {
            chunkId = size / 1000000 + "m";
        } else if (size % 1000 == 0 && size / 1000 != 0) {
            chunkId = size / 1000 + "k";
        } else {
            chunkId = size + "";
        }

        return String.format("%s_%d_%s", regionCoverage.getChromosome(), regionCoverage.getStart() / size, chunkId);
    }


    @Override
    public RegionCoverage convertToDataModelType(DBObject dbObject) {
        String[] split = dbObject.get(CoverageMongoDBWriter.ID_FIELD).toString().split("_");

        RegionCoverage regionCoverage = new RegionCoverage();

        BasicDBList coverageList;
        if (dbObject.containsField(CoverageMongoDBWriter.FILES_FIELD)) {
            coverageList = (BasicDBList) ((BasicDBObject) ((BasicDBList) dbObject.get(CoverageMongoDBWriter.FILES_FIELD)).get(0))
                    .get(CoverageMongoDBWriter.COVERAGE_FIELD);
        } else if (dbObject.containsField(CoverageMongoDBWriter.COVERAGE_FIELD)) {
            coverageList = (BasicDBList) dbObject.get(CoverageMongoDBWriter.FILES_FIELD);
        } else {
            //TODO: Show a error message
            return null;
        }

        short[] all = new short[coverageList.size()];
        int i = 0;
        for (Object o : coverageList) {
            all[i++] = ((Integer) o).shortValue();
        }
//        short[] all = (short[]) ((BasicDBObject) ((BasicDBList) dbObject.get(CoverageMongoWriter.FILES_FIELD)).get(0)).get
// (CoverageMongoWriter.COVERAGE_FIELD);
        //short[] all = (short[]) dbObject.get(CoverageMongoWriter.COVERAGE_FIELD);
        regionCoverage.setAll(all);
        regionCoverage.setChromosome(split[0]);
        regionCoverage.setStart(Integer.parseInt(split[1]) * all.length + 1);
        return regionCoverage;
    }

    @Override
    public DBObject convertToStorageType(RegionCoverage regionCoverage) {
        int size = regionCoverage.getAll().length;

        String chunkId;
        if (size % 1000000 == 0 && size / 1000000 != 0) {
            chunkId = size / 1000000 + "m";
        } else if (size % 1000 == 0 && size / 1000 != 0) {
            chunkId = size / 1000 + "k";
        } else {
            chunkId = size + "";
        }

        //return new BasicDBObject(ID_FIELD, String.format("%s_%d_%s", regionCoverage.getChromosome(), regionCoverage.getStart() / size,
        // chunkId)).
        //        append(FILES_FIELD, Arrays.asList(new BasicDBObject(COVERAGE_FIELD, regionCoverage.getAll())));
        return new BasicDBObject(CoverageMongoDBWriter.COVERAGE_FIELD, regionCoverage.getAll());
    }
}
