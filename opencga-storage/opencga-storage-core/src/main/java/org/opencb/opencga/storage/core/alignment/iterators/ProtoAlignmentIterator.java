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

package org.opencb.opencga.storage.core.alignment.iterators;

import ga4gh.Reads;
import org.opencb.biodata.tools.alignment.iterators.BamIterator;

/**
 * Created by pfurio on 26/10/16.
 */
public class ProtoAlignmentIterator extends AlignmentIterator<Reads.ReadAlignment> {

    private BamIterator<Reads.ReadAlignment> protoIterator;

    public ProtoAlignmentIterator(BamIterator<Reads.ReadAlignment> protoIterator) {
        this.protoIterator = protoIterator;
    }

    @Override
    public void close() throws Exception {
        protoIterator.close();
    }

    @Override
    public boolean hasNext() {
        return protoIterator.hasNext();
    }

    @Override
    public Reads.ReadAlignment next() {
        return protoIterator.next();
    }
}
