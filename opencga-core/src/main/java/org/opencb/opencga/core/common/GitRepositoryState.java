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

package org.opencb.opencga.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created on 27/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GitRepositoryState {

    public static final String DEFAULT_RESOURCE_NAME = "org/opencb/opencga/core/git.properties";
    private static GitRepositoryState gitRepositoryState;
    private static final Logger logger = LoggerFactory.getLogger(GitRepositoryState.class);

    private Properties properties;
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

    public static String get(String key) {
        return getInstance().properties.getProperty(key);
    }

    public static GitRepositoryState getInstance() {
        if (gitRepositoryState == null) {
            gitRepositoryState = load(DEFAULT_RESOURCE_NAME);
        }
        return gitRepositoryState;
    }

    public static GitRepositoryState load(String resourceName) {
        Properties properties = new Properties();
        InputStream stream = null;
        try {
            stream = GitRepositoryState.class.getClassLoader().getResourceAsStream(resourceName);
            if (stream != null) {
                properties.load(stream);
            }
        } catch (IOException e) {
            logger.warn("Error reading " + resourceName, e);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    logger.warn("Error closing stream from " + resourceName, e);
                }
            }
        }
        return new GitRepositoryState(properties);
    }

    GitRepositoryState() {
    }

    private GitRepositoryState(Properties properties) {
        this.properties = properties;
        this.tags = properties.getProperty("git.tags");
        this.branch = properties.getProperty("git.branch");
        this.dirty = properties.getProperty("git.dirty");
        this.remoteOriginUrl = properties.getProperty("git.remote.origin.url");

        this.commitId = properties.getProperty("git.commit.id"); // OR properties.get("git.commit.id") depending on your configuration
        this.commitIdAbbrev = properties.getProperty("git.commit.id.abbrev");
        this.describe = properties.getProperty("git.commit.id.describe");
        this.describeShort = properties.getProperty("git.commit.id.describe-short");
        this.commitUserName = properties.getProperty("git.commit.user.name");
        this.commitUserEmail = properties.getProperty("git.commit.user.email");
        this.commitMessageFull = properties.getProperty("git.commit.message.full");
        this.commitMessageShort = properties.getProperty("git.commit.message.short");
        this.commitTime = properties.getProperty("git.commit.time");
        this.closestTagName = properties.getProperty("git.closest.tag.name");
        this.closestTagCommitCount = properties.getProperty("git.closest.tag.commit.count");

        this.buildUserName = properties.getProperty("git.build.user.name");
        this.buildUserEmail = properties.getProperty("git.build.user.email");
        this.buildTime = properties.getProperty("git.build.time");
        this.buildHost = properties.getProperty("git.build.host");
        this.buildVersion = properties.getProperty("git.build.version");
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
