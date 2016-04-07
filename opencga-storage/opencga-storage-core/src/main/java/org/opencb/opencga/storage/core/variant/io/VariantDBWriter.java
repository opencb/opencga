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

package org.opencb.opencga.storage.core.variant.io;

import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;

import java.util.List;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public abstract class VariantDBWriter implements VariantWriter {

    protected abstract boolean buildBatchRaw(List<Variant> data);

    protected abstract boolean buildEffectRaw(List<Variant> variants);

    protected abstract boolean buildBatchIndex(List<Variant> data);

    protected abstract boolean writeBatch(List<Variant> data);

}
