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

package org.opencb.opencga.core.config;

/**
 * Created by pfurio on 07/04/16.
 */
@Deprecated
public class Policies {

    private UserCreation userCreation;

    public Policies() {
        this.userCreation = UserCreation.ALWAYS;
    }

    public UserCreation getUserCreation() {
        return userCreation;
    }

    public Policies setUserCreation(UserCreation userCreation) {
        this.userCreation = userCreation;
        return this;
    }

    public enum UserCreation {
        ONLY_ADMIN, ANY_LOGGED_USER, ALWAYS
    }

}
