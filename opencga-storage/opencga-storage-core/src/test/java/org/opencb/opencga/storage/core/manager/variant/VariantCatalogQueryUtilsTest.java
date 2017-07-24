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

package org.opencb.opencga.storage.core.manager.variant;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.managers.CatalogManager;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDIES;

/**
 * Created on 28/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantCatalogQueryUtilsTest {

    private CatalogManager catalogManager;

    private Map<String, Long> studyNameMap;
    private VariantCatalogQueryUtils catalogUtils;

    @Before
    public void setUp() throws Exception {
        studyNameMap = new HashMap<>();
        addStudyId(1L);
        addStudyId(2L);
        addStudyId(3L);

        catalogManager = mock(CatalogManager.class);
        doAnswer(invocation -> studyNameMap.get(invocation.getArgument(0).toString()))
                .when(catalogManager).getStudyId(anyString(), anyString());
        catalogUtils = new VariantCatalogQueryUtils(catalogManager);
    }

    private void addStudyId(long studyId) {
        studyNameMap.put(String.valueOf(studyId), studyId);
        studyNameMap.put("s"+studyId, studyId);
        studyNameMap.put("p1:s"+studyId, studyId);
        studyNameMap.put("u@p1:s"+studyId, studyId);
    }

    @Test
    public void parseQuery() throws Exception {
        assertEquals("1", catalogUtils.parseQuery(new Query(STUDIES.key(), "s1"), "sessionId").getString(STUDIES.key()));
        assertEquals("1,2", catalogUtils.parseQuery(new Query(STUDIES.key(), "s1,s2"), "sessionId").getString(STUDIES.key()));
        assertEquals("!1,2", catalogUtils.parseQuery(new Query(STUDIES.key(), "!s1,2"), "sessionId").getString(STUDIES.key()));
        assertEquals("2;!1;3", catalogUtils.parseQuery(new Query(STUDIES.key(), "u@p1:s2;!s1;p1:s3"), "sessionId").getString(STUDIES.key()));
    }

}