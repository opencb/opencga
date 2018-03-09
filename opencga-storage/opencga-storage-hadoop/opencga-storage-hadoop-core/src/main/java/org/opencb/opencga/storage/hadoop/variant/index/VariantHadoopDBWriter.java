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

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 30/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopDBWriter extends AbstractHBaseDataWriter<Variant, Put> {

    private final StudyEntryToHBaseConverter converter;

    public VariantHadoopDBWriter(GenomeHelper helper, String tableName, StudyConfiguration sc, HBaseManager hBaseManager) {
        super(hBaseManager, tableName);
        converter = new StudyEntryToHBaseConverter(helper.getColumnFamily(), sc, true);
    }

    @Override
    protected List<Put> convert(List<Variant> list) {
        List<Put> puts = new ArrayList<>(list.size());
        for (Variant variant : list) {
            if (HadoopVariantStorageEngine.TARGET_VARIANT_TYPE_SET.contains(variant.getType())) {
                Put put = converter.convert(variant);
                puts.add(put);
            } //Discard ref_block and symbolic variants.
        }
        return puts;
    }

}
