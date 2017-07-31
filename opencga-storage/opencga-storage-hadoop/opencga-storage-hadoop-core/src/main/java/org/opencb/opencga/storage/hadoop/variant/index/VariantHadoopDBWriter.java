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

package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.samples.SamplesDataToHBaseConverter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 30/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopDBWriter implements DataWriter<Variant> {

    private final String tableName;
    private final SamplesDataToHBaseConverter converter;
    private final HBaseManager hBaseManager;
    private BufferedMutator tableMutator;

    public VariantHadoopDBWriter(VariantTableHelper helper, StudyConfiguration sc, HBaseManager hBaseManager) {
        this(helper, helper.getAnalysisTableAsString(), sc, hBaseManager);
    }

    public VariantHadoopDBWriter(GenomeHelper helper, String tableName, StudyConfiguration sc, HBaseManager hBaseManager) {
        this.tableName = tableName;
        converter = new SamplesDataToHBaseConverter(helper.getColumnFamily(), sc, true);
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(helper.getConf());
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }

    }

    @Override
    public boolean open() {
        try {
            tableMutator = hBaseManager.getConnection().getBufferedMutator(TableName.valueOf(this.tableName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            tableMutator.flush();
            tableMutator.close();
            hBaseManager.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean write(List<Variant> list) {
        List<Put> puts = new ArrayList<>(list.size());
        for (Variant variant : list) {
            if (VariantMergerTableMapper.TARGET_VARIANT_TYPE_SET.contains(variant.getType())) {
                Put put = converter.convert(variant);
                puts.add(put);
            } //Discard ref_block and symbolic variants.
        }
        try {
            tableMutator.mutate(puts);
        } catch (IOException e) {
            throw new UncheckedIOException("Error loading variants", e);
        }
        return true;
    }
}
