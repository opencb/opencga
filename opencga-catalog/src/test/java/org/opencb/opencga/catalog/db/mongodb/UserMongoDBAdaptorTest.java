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

package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserFilter;
import org.opencb.opencga.core.models.user.UserInternal;
import org.opencb.opencga.core.models.user.UserStatus;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 19/01/16.
 */
@Category(MediumTests.class)
public class UserMongoDBAdaptorTest extends AbstractMongoDBAdaptorTest {

    @Test
    public void nativeGet() throws Exception {
        Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), normalUserId1);
        DataResult<Document> queryResult = catalogUserDBAdaptor.nativeGet(query, null);
        assertEquals(1, queryResult.getNumResults());
        assertEquals(normalUserId1, queryResult.first().getString(UserDBAdaptor.QueryParams.ID.key()));
    }

    @Test
    public void createUserTest() throws CatalogException {
        User user = new User("NewUser", "", "", "", new UserInternal(new UserStatus()));
        OpenCGAResult insert = catalogUserDBAdaptor.insert(user, "", null);
        assertEquals(1, insert.getNumInserted());

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("already exists");
        catalogUserDBAdaptor.insert(user, "", null);
    }

    @Test
    public void deleteUserTest() throws CatalogException {
        User deletable1 = new User("deletable1", "deletable 1", "d1@ebi", "", new UserInternal(new UserStatus()));
        catalogUserDBAdaptor.insert(deletable1, "1234", null);
        Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), "deletable1");
        DataResult<User> userResult = catalogUserDBAdaptor.get(query, QueryOptions.empty());
        assertEquals(1, userResult.getNumResults());
        assertEquals("deletable1", userResult.first().getId());
        assertEquals(InternalStatus.READY, userResult.first().getInternal().getStatus().getId());

        DataResult deleteUser = catalogUserDBAdaptor.delete(deletable1.getId(), new QueryOptions());
        assertEquals(1, deleteUser.getNumUpdated());

        query.append(UserDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), UserStatus.DELETED);
        DataResult<User> queryResult = catalogUserDBAdaptor.get(query, QueryOptions.empty());
        assertEquals(InternalStatus.DELETED, queryResult.first().getInternal().getStatus().getId());
    }

    @Test
    public void getUserTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DataResult<User> user = catalogUserDBAdaptor.get(normalUserId1, null);
        assertEquals(1, user.getNumResults());
        assertEquals(normalUserId1, user.first().getId());

        user = catalogUserDBAdaptor.get(normalUserId3, null);
        assertEquals(1, user.getNumResults());
        assertEquals(normalUserId3, user.first().getId());

        OpenCGAResult<User> nonExistingUser = catalogUserDBAdaptor.get("NonExistingUser", null);
        assertEquals(0, nonExistingUser.getNumResults());
    }

    @Test
    public void changePasswordTest() throws CatalogDBException, CatalogAuthenticationException {
        DataResult result = catalogUserDBAdaptor.changePassword(normalUserId1, TestParamConstants.PASSWORD, "1234");
        assertEquals(1, result.getNumUpdated());

        thrown.expect(CatalogAuthenticationException.class);
        catalogUserDBAdaptor.changePassword(normalUserId1, "BAD_PASSWORD", "asdf");
    }

    @Test
    public void modifyUserTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        ObjectMap genomeMapsConfig = new ObjectMap("lastPosition", "4:1222222:1333333");
        genomeMapsConfig.put("otherConf", Arrays.asList(1, 2, 3, 4, 5));
        catalogUserDBAdaptor.setConfig(normalUserId1, "genomemaps", genomeMapsConfig);
        catalogUserDBAdaptor.setConfig(normalUserId1, "genomemaps2", genomeMapsConfig);

        User user = catalogUserDBAdaptor.get(normalUserId1, null).first();
        assertNotNull(user.getConfigs().get("genomemaps"));
        assertNotNull(user.getConfigs().get("genomemaps2"));
        Map<String, Object> genomemaps = user.getConfigs().get("genomemaps");
        assertNotNull(genomemaps.get("otherConf"));
        assertNotNull(genomemaps.get("lastPosition"));

        catalogUserDBAdaptor.deleteConfig(normalUserId1, "genomemaps");
        user = catalogUserDBAdaptor.get(normalUserId1, null).first();
        assertNull(user.getConfigs().get("genomemaps"));
        assertNotNull(user.getConfigs().get("genomemaps2"));
    }

    @Test
    public void addFilterTest() throws CatalogDBException, IOException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query("key1", "value1").append("key2", "value2");
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("key1", "key2"));
        UserFilter filter = new UserFilter("filter1", "Description of filter 1", Enums.Resource.ALIGNMENT, query, options);

        catalogUserDBAdaptor.addFilter(normalUserId1, filter);
        DataResult<User> userDataResult = catalogUserDBAdaptor.get(normalUserId1, new QueryOptions());

        UserFilter filterResult = userDataResult.first().getFilters().get(0);

        assertEquals(filter.getId(), filterResult.getId());
        assertEquals(filter.getDescription(), filterResult.getDescription());
        assertEquals(filter.getResource(), filterResult.getResource());
        assertEquals(filter.getQuery().safeToString(), filterResult.getQuery().safeToString());
        assertEquals(filter.getOptions().safeToString(), filterResult.getOptions().safeToString());
    }

    @Test
    public void updateFilterTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query("key1", "value1").append("key2", "value2");
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("key1", "key2"));
        UserFilter filter = new UserFilter("filter1", "Description of filter 1", Enums.Resource.ALIGNMENT, query, options);
        catalogUserDBAdaptor.addFilter(normalUserId1, filter);

        ObjectMap params = new ObjectMap()
                .append(UserDBAdaptor.FilterParams.DESCRIPTION.key(), "The description has changed")
                .append(UserDBAdaptor.FilterParams.RESOURCE.key(), Enums.Resource.VARIANT)
                .append(UserDBAdaptor.FilterParams.QUERY.key(), new Query("key3", "whatever"))
                .append(UserDBAdaptor.FilterParams.OPTIONS.key(), new QueryOptions("options", "optionsValue"));
        catalogUserDBAdaptor.updateFilter(normalUserId1, filter.getId(), params);

        DataResult<User> userDataResult = catalogUserDBAdaptor.get(normalUserId1, new QueryOptions());

        UserFilter filterResult = userDataResult.first().getFilters().get(0);

        assertEquals(filter.getId(), filterResult.getId());
        assertEquals(params.get(UserDBAdaptor.FilterParams.DESCRIPTION.key()), filterResult.getDescription());
        assertEquals(params.get(UserDBAdaptor.FilterParams.RESOURCE.key()), filterResult.getResource());
        assertEquals(((Query) params.get(UserDBAdaptor.FilterParams.QUERY.key())).safeToString(), filterResult.getQuery().safeToString());
        assertEquals(((QueryOptions) params.get(UserDBAdaptor.FilterParams.OPTIONS.key())).safeToString(),
                filterResult.getOptions().safeToString());
    }

    @Test
    public void deleteFilterTest() throws CatalogDBException, IOException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query("key1", "value1").append("key2", "value2");
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("key1", "key2"));
        UserFilter filter = new UserFilter("filter1", "Description of filter 1", Enums.Resource.ALIGNMENT, query, options);

        catalogUserDBAdaptor.addFilter(normalUserId1, filter);
        catalogUserDBAdaptor.deleteFilter(normalUserId1, filter.getId());
        DataResult<User> userDataResult = catalogUserDBAdaptor.get(normalUserId1, new QueryOptions());

        List<UserFilter> filters = userDataResult.first().getFilters();
        assertTrue(filters.size() == 0);
    }


    @Test
    public void setConfigTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        ObjectMap objectMap = new ObjectMap()
                .append("key1", Arrays.asList(1, 2, 3, 4, 5))
                .append("key2", new ObjectMap("key21", 21).append("key22", 22));

        DataResult writeResult = catalogUserDBAdaptor.setConfig(normalUserId1, "config1", objectMap);

        assertEquals(1, writeResult.getNumUpdated());

        DataResult<User> queryResult = catalogUserDBAdaptor.get(normalUserId1, QueryOptions.empty());
        ObjectMap result = queryResult.first().getConfigs().get("config1");
        assertTrue(result.get("key1") instanceof List);
        assertTrue(result.get("key2") instanceof Map);

        // Update the config
        objectMap.put("key2", objectMap.get("key1"));
        writeResult = catalogUserDBAdaptor.setConfig(normalUserId1, "config1", objectMap);
        assertEquals(1, writeResult.getNumUpdated());

        queryResult = catalogUserDBAdaptor.get(normalUserId1, QueryOptions.empty());
        result = queryResult.first().getConfigs().get("config1");

        assertTrue(result.get("key1") instanceof List);
        assertTrue(result.get("key2") instanceof List);
    }

    @Test
    public void deleteConfigTest() throws CatalogDBException, IOException {
        ObjectMap objectMap = new ObjectMap()
                .append("key1", Arrays.asList(1, 2, 3, 4, 5))
                .append("key2", new ObjectMap("key21", 21).append("key22", 22));

        catalogUserDBAdaptor.setConfig(normalUserId1, "config1", objectMap);

        catalogUserDBAdaptor.deleteConfig(normalUserId1, "config1");

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("Could not delete config1 configuration");
        catalogUserDBAdaptor.deleteConfig(normalUserId1, "config1");
    }


}
