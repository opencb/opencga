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

package org.opencb.opencga.core.tools.result;

import org.opencb.commons.utils.GitRepositoryState;

import java.util.Date;

public class Status {

    public enum Type {
        /**
         * PENDING status: The job or step has not started yet.
         */
        PENDING,

        /**
         * RUNNING status: The job or step is running.
         */
        RUNNING,

        /**
         * DONE status: The job or step has finished the execution, but the output is still not ready.
         */
        DONE,

        /**
         * ERROR status: The job or step finished with an error.
         */
        ERROR
    }

    private Type name;
    private String step;
    private Date date;
    private String version;
    private String commit;

    protected Status() {
    }

    public Status(Type name, String step, Date date) {
        this.name = name;
        this.step = step;
        this.date = date;
    }

    public static Status initStatus() {
        Status status = new Status();
        status.version = GitRepositoryState.get().getBuildVersion();
        status.commit = GitRepositoryState.get().getCommitId();
        return status;
    }

    public Type getName() {
        return name;
    }

    public Status setName(Type name) {
        this.name = name;
        return this;
    }

    public String getStep() {
        return step;
    }

    public Status setStep(String step) {
        this.step = step;
        return this;
    }

    public Date getDate() {
        return date;
    }

    public Status setDate(Date date) {
        this.date = date;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public Status setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getCommit() {
        return commit;
    }

    public Status setCommit(String commit) {
        this.commit = commit;
        return this;
    }
}
