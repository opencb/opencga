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

package org.opencb.opencga.core.common;

import java.io.IOException;
import java.util.Properties;

/**
 * Created on 27/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GitRepositoryState {

    private static GitRepositoryState gitRepositoryState;

    private String tags;                    // =${git.tags} // comma separated tag names
    private String branch;                  // =${git.branch}
    private String dirty;                   // =${git.dirty}
    private String remoteOriginUrl;         // =${git.remote.origin.url}

    private String commitId;                // =${git.commit.id.full} OR ${git.commit.id}
    private String commitIdAbbrev;          // =${git.commit.id.abbrev}
    private String describe;                // =${git.commit.id.describe}
    private String describeShort;           // =${git.commit.id.describe-short}
    private String commitUserName;          // =${git.commit.user.name}
    private String commitUserEmail;         // =${git.commit.user.email}
    private String commitMessageFull;       // =${git.commit.message.full}
    private String commitMessageShort;      // =${git.commit.message.short}
    private String commitTime;              // =${git.commit.time}
    private String closestTagName;          // =${git.closest.tag.name}
    private String closestTagCommitCount;   // =${git.closest.tag.commit.count}

    private String buildUserName;           // =${git.build.user.name}
    private String buildUserEmail;          // =${git.build.user.email}
    private String buildTime;               // =${git.build.time}
    private String buildHost;               // =${git.build.host}
    private String buildVersion;            // =${git.build.version}

    public static GitRepositoryState get() {
        if (gitRepositoryState == null) {
            Properties properties = new Properties();
            try {
                properties.load(GitRepositoryState.class.getClassLoader().getResourceAsStream("org/opencb/opencga/core/git.properties"));
            } catch (IOException e) {
                e.printStackTrace();
            }

            gitRepositoryState = new GitRepositoryState(properties);
        }
        return gitRepositoryState;
    }

    GitRepositoryState() {
    }

    private GitRepositoryState(Properties properties) {
        this.tags = properties.get("git.tags").toString();
        this.branch = properties.get("git.branch").toString();
        this.dirty = properties.get("git.dirty").toString();
        this.remoteOriginUrl = properties.get("git.remote.origin.url").toString();

        this.commitId = properties.get("git.commit.id").toString(); // OR properties.get("git.commit.id") depending on your configuration
        this.commitIdAbbrev = properties.get("git.commit.id.abbrev").toString();
        this.describe = properties.get("git.commit.id.describe").toString();
        this.describeShort = properties.get("git.commit.id.describe-short").toString();
        this.commitUserName = properties.get("git.commit.user.name").toString();
        this.commitUserEmail = properties.get("git.commit.user.email").toString();
        this.commitMessageFull = properties.get("git.commit.message.full").toString();
        this.commitMessageShort = properties.get("git.commit.message.short").toString();
        this.commitTime = properties.get("git.commit.time").toString();
        this.closestTagName = properties.get("git.closest.tag.name").toString();
        this.closestTagCommitCount = properties.get("git.closest.tag.commit.count").toString();

        this.buildUserName = properties.get("git.build.user.name").toString();
        this.buildUserEmail = properties.get("git.build.user.email").toString();
        this.buildTime = properties.get("git.build.time").toString();
        this.buildHost = properties.get("git.build.host").toString();
        this.buildVersion = properties.get("git.build.version").toString();
    }

    @Override
    public String toString() {
        return "{\n" +
                    "\ttags : '" + tags + '\'' + ",\n" +
                    "\tbranch : '" + branch + '\'' + ",\n" +
                    "\tdirty : '" + dirty + '\'' + ",\n" +
                    "\tremoteOriginUrl : '" + remoteOriginUrl + '\'' + ",\n" +
                    "\tcommitId : '" + commitId + '\'' + ",\n" +
                    "\tcommitIdAbbrev : '" + commitIdAbbrev + '\'' + ",\n" +
                    "\tdescribe : '" + describe + '\'' + ",\n" +
                    "\tdescribeShort : '" + describeShort + '\'' + ",\n" +
                    "\tcommitUserName : '" + commitUserName + '\'' + ",\n" +
                    "\tcommitUserEmail : '" + commitUserEmail + '\'' + ",\n" +
                    "\tcommitMessageFull : '" + commitMessageFull + '\'' + ",\n" +
                    "\tcommitMessageShort : '" + commitMessageShort + '\'' + ",\n" +
                    "\tcommitTime : '" + commitTime + '\'' + ",\n" +
                    "\tclosestTagName : '" + closestTagName + '\'' + ",\n" +
                    "\tclosestTagCommitCount : '" + closestTagCommitCount + '\'' + ",\n" +
                    "\tbuildUserName : '" + buildUserName + '\'' + ",\n" +
                    "\tbuildUserEmail : '" + buildUserEmail + '\'' + ",\n" +
                    "\tbuildTime : '" + buildTime + '\'' + ",\n" +
                    "\tbuildHost : '" + buildHost + '\'' + ",\n" +
                    "\tbuildVersion : '" + buildVersion + '\'' + ",\n" +
                '}';
    }

    /* Generate setters and getters here */
    public static GitRepositoryState getGitRepositoryState() {
        return gitRepositoryState;
    }

    public String getTags() {
        return tags;
    }

    public String getBranch() {
        return branch;
    }

    public String getDirty() {
        return dirty;
    }

    public String getRemoteOriginUrl() {
        return remoteOriginUrl;
    }

    public String getCommitId() {
        return commitId;
    }

    public String getCommitIdAbbrev() {
        return commitIdAbbrev;
    }

    public String getDescribe() {
        return describe;
    }

    public String getDescribeShort() {
        return describeShort;
    }

    public String getCommitUserName() {
        return commitUserName;
    }

    public String getCommitUserEmail() {
        return commitUserEmail;
    }

    public String getCommitMessageFull() {
        return commitMessageFull;
    }

    public String getCommitMessageShort() {
        return commitMessageShort;
    }

    public String getCommitTime() {
        return commitTime;
    }

    public String getClosestTagName() {
        return closestTagName;
    }

    public String getClosestTagCommitCount() {
        return closestTagCommitCount;
    }

    public String getBuildUserName() {
        return buildUserName;
    }

    public String getBuildUserEmail() {
        return buildUserEmail;
    }

    public String getBuildTime() {
        return buildTime;
    }

    public String getBuildHost() {
        return buildHost;
    }

    public String getBuildVersion() {
        return buildVersion;
    }
}
