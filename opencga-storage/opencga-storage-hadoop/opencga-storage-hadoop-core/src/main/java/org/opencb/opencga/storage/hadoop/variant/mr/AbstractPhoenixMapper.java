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

package org.opencb.opencga.storage.hadoop.variant.mr;

import com.google.common.collect.BiMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mh719 on 22/12/2016.
 */
public class AbstractPhoenixMapper<PHOENIXIN, KEYOUT, VALUEOUT> extends Mapper<NullWritable, PHOENIXIN, KEYOUT, VALUEOUT> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final AtomicReference<VariantsTableMapReduceHelper> mrHelper = new AtomicReference<>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mrHelper.set(new VariantsTableMapReduceHelper(context));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        getMrHelper().close();
    }

    public VariantsTableMapReduceHelper getMrHelper() {
        return mrHelper.get();
    }

    public VariantTableHelper getHelper() {
        return getMrHelper().getHelper();
    }

    public HBaseManager getHBaseManager() {
        return getMrHelper().getHBaseManager();
    }

    public long getTimestamp() {
        return getMrHelper().getTimestamp();
    }

    public BiMap<String, Integer> getIndexedSamples() {
        return getMrHelper().getIndexedSamples();
    }

    public StudyConfiguration getStudyConfiguration() {
        return getMrHelper().getStudyConfiguration();
    }

    public HBaseToVariantConverter getHbaseToVariantConverter() {
        return getMrHelper().getHbaseToVariantConverter();
    }

    public void startStep() {
        getMrHelper().startStep();
    }

    public void endStep(String name) {
        getMrHelper().endStep(name);
    }
}
