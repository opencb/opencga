/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.audit;


import org.opencb.opencga.catalog.db.AbstractDBAdaptor;

import static org.opencb.opencga.catalog.db.AbstractDBAdaptor.FilterOption.Type.TEXT;

/**
 * Created on 18/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public enum AuditFilterOption implements AbstractDBAdaptor.FilterOption {
    id("id", "", TEXT),
    timeStamp("timeStamp", "", TEXT),
    resource("resource", "", TEXT),
    action("action", "", TEXT),
    userId("userId", "", TEXT);

    private final String _key;
    private final String _description;
    private final Type _type;

    AuditFilterOption(String key, String description, Type type) {
        _key = key;
        _description = description;
        _type = type;
    }

    @Override
    public String getKey() {
        return _key;
    }

    @Override
    public String getDescription() {
        return _description;
    }

    @Override
    public Type getType() {
        return _type;
    }

}
