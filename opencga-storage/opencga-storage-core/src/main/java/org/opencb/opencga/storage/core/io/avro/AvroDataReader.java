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

package org.opencb.opencga.storage.core.io.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.opencb.commons.io.DataReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 09/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 * @deprecated Use {@link org.opencb.commons.io.avro.AvroDataReader}
 */
@Deprecated
public class AvroDataReader<T extends GenericRecord> implements DataReader<T> {

    private final Class<T> clazz;
    private final File file;
    private DataFileReader<T> dataFileReader;

    public AvroDataReader(File file, Class<T> clazz) {
        this.clazz = clazz;
        this.file = file;
    }


    @Override
    public boolean open() {
        DatumReader<T> datumReader = new SpecificDatumReader<>(clazz);
        try {
            dataFileReader = new DataFileReader<>(file, datumReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            dataFileReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public List<T> read(int batchSize) {
        List<T> batch = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize && dataFileReader.hasNext(); i++) {
            batch.add(dataFileReader.next());
        }
        return batch;
    }
}
