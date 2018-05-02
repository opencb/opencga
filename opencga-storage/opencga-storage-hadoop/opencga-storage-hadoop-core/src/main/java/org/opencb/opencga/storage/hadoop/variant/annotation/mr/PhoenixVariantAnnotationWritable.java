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

package org.opencb.opencga.storage.hadoop.variant.annotation.mr;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mh719 on 15/12/2016.
 */
public class PhoenixVariantAnnotationWritable implements DBWritable, Writable {
    private final List<Object> orderedValues;
    private final AtomicReference<ResultSet> resultSet = new AtomicReference<>();

    public PhoenixVariantAnnotationWritable(final List<Object> orderedValues) {
        this.orderedValues = orderedValues;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // do nothing
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // do nothing
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        Integer i = 1;
        for (Object value : this.orderedValues) {
            preparedStatement.setObject(i++, value);
        }
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.resultSet.set(resultSet);
    }

    public ResultSet getResultSet() {
        return resultSet.get();
    }
}
