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

package org.opencb.opencga.catalog.models;

/**
 * Created on 21/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public class StudyPermissions {

    /**
     * Define the default permission for read resources.
     */
    private boolean read;
    /**
     * Define the default permission for write or update resources.
     */
    private boolean write;
    /**
     * Define the default permission for delete resources.
     */
    private boolean delete;
    /**
     * Define if the group members are authorized to launch or execute jobs.
     * Also, will need READ permission for the input jobs, and WRITE permission for the output directory.
     */
    private boolean launchJobs;
    /**
     * Define if the group members are authorized to delete jobs from the db.
     */
    private boolean deleteJobs;
    /**
     * Set the group as Sample Manager.
     * Define a set of permissions:
     * Create, read, update and delete operations over all Samples
     * Create, read, update and delete operations over all Individuals
     * Create, read, update and delete operations over all Cohorts
     * Create, read, update and delete operations over all VariableSets
     */
    private boolean managerSamples;
    /**
     * Set the group as Study Manager.
     * Define a set of permissions:
     * Edit Study metadata information
     * Create new Groups
     * Add or remove users to a group
     * Change group permissions
     * Change resource ACLs
     */
    private boolean studyManager;

    public StudyPermissions() {
    }

    public StudyPermissions(boolean read, boolean write, boolean delete, boolean launchJobs, boolean deleteJobs, boolean managerSamples,
                            boolean studyManager) {
        this.read = read;
        this.write = write;
        this.delete = delete;
        this.launchJobs = launchJobs;
        this.deleteJobs = deleteJobs;
        this.managerSamples = managerSamples;
        this.studyManager = studyManager;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyPermissions{");
        sb.append("read=").append(read);
        sb.append(", write=").append(write);
        sb.append(", delete=").append(delete);
        sb.append(", launchJobs=").append(launchJobs);
        sb.append(", deleteJobs=").append(deleteJobs);
        sb.append(", managerSamples=").append(managerSamples);
        sb.append(", studyManager=").append(studyManager);
        sb.append('}');
        return sb.toString();
    }

    public boolean isRead() {
        return read;
    }

    public StudyPermissions setRead(boolean read) {
        this.read = read;
        return this;
    }

    public boolean isWrite() {
        return write;
    }

    public StudyPermissions setWrite(boolean write) {
        this.write = write;
        return this;
    }

    public boolean isDelete() {
        return delete;
    }

    public StudyPermissions setDelete(boolean delete) {
        this.delete = delete;
        return this;
    }

    public boolean isLaunchJobs() {
        return launchJobs;
    }

    public StudyPermissions setLaunchJobs(boolean launchJobs) {
        this.launchJobs = launchJobs;
        return this;
    }

    public boolean isDeleteJobs() {
        return deleteJobs;
    }

    public StudyPermissions setDeleteJobs(boolean deleteJobs) {
        this.deleteJobs = deleteJobs;
        return this;
    }

    public boolean isManagerSamples() {
        return managerSamples;
    }

    public StudyPermissions setManagerSamples(boolean managerSamples) {
        this.managerSamples = managerSamples;
        return this;
    }

    public boolean isStudyManager() {
        return studyManager;
    }

    public StudyPermissions setStudyManager(boolean studyManager) {
        this.studyManager = studyManager;
        return this;
    }
}
