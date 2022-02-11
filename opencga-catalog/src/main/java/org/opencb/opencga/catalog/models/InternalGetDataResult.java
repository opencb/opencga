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

package org.opencb.opencga.catalog.models;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InternalGetDataResult<T> extends OpenCGAResult<T> {

    private List<Missing> missing;

    /**
     * When all versions are fetched for several entries, the results will be sorted as usual but here we will write how many documents of
     * every entry were found. Example: User queries sample1 (has 3 versions), sample2 (has only 1 version), sample3 (has 2 versions)
     * The results array would contain a list with: sample1 v2, sample1 v1, sample1 v3, sample2 v1, sample3 v1, sample3 v2
     * And the list of groups would be: 3, 1, 2; indicating that the first 3 samples form the first group of the query, then sample2, and
     * finally two versions of sample3.
     * Entry versions might not be sorted.
     */
    private List<Integer> groups;

    public InternalGetDataResult() {
    }

    public InternalGetDataResult(DataResult<T> dataResult) {
        super(dataResult.getTime(), dataResult.getEvents(), dataResult.getNumResults(), dataResult.getResults(),
                dataResult.getNumMatches(), dataResult.getNumInserted(), dataResult.getNumUpdated(), dataResult.getNumDeleted(),
                dataResult.getNumErrors(), dataResult.getAttributes());
        this.groups = new ArrayList<>();
    }

    public List<Missing> getMissing() {
        return missing != null ? missing : Collections.emptyList();
    }

    public InternalGetDataResult<T> setMissing(List<Missing> missing) {
        this.missing = missing;
        return this;
    }

    public List<Integer> getGroups() {
        return groups != null ? groups : Collections.emptyList();
    }

    public InternalGetDataResult<T> setGroups(List<Integer> groups) {
        this.groups = groups;
        return this;
    }

    public List<List<T>> getVersionedResults() {
        List<T> result = getResults();

        if (groups == null || groups.size() == 1) {
            return Collections.singletonList(result);
        }

        List<List<T>> myResults = new ArrayList<>();
        int counter = 0;
        for (int total : groups) {
            List<T> auxList = new ArrayList<>(total);

            for (int i = 0; i < total; i++) {
                auxList.add(result.get(counter + i));
            }
            myResults.add(auxList);
            counter += total;
        }

        return myResults;
    }

    public void addMissing(String id, String errorMsg) {
        if (this.missing == null) {
            this.missing = new ArrayList<>();
        }

        this.missing.add(new Missing(id, errorMsg));
    }

    public class Missing {
        private String id;
        private String errorMsg;

        public Missing(String id, String errorMsg) {
            this.id = id;
            this.errorMsg = errorMsg;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Missing{");
            sb.append("id='").append(id).append('\'');
            sb.append(", errorMsg='").append(errorMsg).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public String getErrorMsg() {
            return errorMsg;
        }
    }

}
