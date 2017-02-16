/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
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
 * </p>
 */

package com.dangdang.ddframe.job.lite.internal.election;

import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;

/**
 * Elastic Job主服务器根节点名称的常量类.
 *
 * @author zhangliang
 */
public final class ElectionNode {

    /**
     * Elastic Job主服务器根节点.
     */
    public static final String ROOT = "leader";

    /** leader/election */
    static final String ELECTION_ROOT = ROOT + "/election";

    /** leader/election/host */
    static final String LEADER_HOST = ELECTION_ROOT + "/host";

    /**  leader/election/latch */
    static final String LATCH = ELECTION_ROOT + "/latch";

    private final JobNodePath jobNodePath;

    ElectionNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }

    /**
     * 判断传入的path是否为   /jobName/leader/election/host
     *
     * @param path
     * @return
     */
    boolean isLeaderHostPath(final String path) {
        // leader/election/host
        return jobNodePath.getFullPath(LEADER_HOST).equals(path);
    }
}
