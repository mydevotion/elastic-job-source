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

import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.server.ServerNode;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 主节点选举监听管理器.
 * <p>
 * 选举leader，以及因为leader被加入到服务禁用，服务shutdown等删除leader等操作
 *
 * @author zhangliang
 */
@Slf4j
public class ElectionListenerManager extends AbstractListenerManager {

    private final LeaderElectionService leaderElectionService;

    private final ServerService serverService;

    private final ElectionNode electionNode;

    private final ServerNode serverNode;


    public ElectionListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        leaderElectionService = new LeaderElectionService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        electionNode = new ElectionNode(jobName);
        serverNode = new ServerNode(jobName);
    }

    @Override
    public void start() {
        addDataListener(new LeaderElectionJobListener());
    }

    class LeaderElectionJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final CuratorFramework client, final TreeCacheEvent event, final String path) {
            EventHelper eventHelper = new EventHelper(path, event);
            if (eventHelper.isLeaderCrashedOrServerOn() && !leaderElectionService.hasLeader() && !serverService.getAvailableServers().isEmpty()) {
                log.debug("Leader crashed, elect a new leader now.");
                // 判断节点是否改变，如果改变，则重新选择leader
                leaderElectionService.leaderElection();
                log.debug("Leader election completed.");
                return;
            }
            if (eventHelper.isServerOff() && leaderElectionService.isLeader()) {
                // 如果当前leader被加入到禁用服务、暂停或者shutdown状态，则删除leader节点
                leaderElectionService.removeLeader();
            }
        }

        @RequiredArgsConstructor
        final class EventHelper {

            private final String path;

            private final TreeCacheEvent event;

            /**
             * 判断leader是否已经宕机，禁用或者终止
             *
             * @return
             */
            boolean isLeaderCrashedOrServerOn() {
                return isLeaderCrashed() || isServerEnabled() || isServerResumed();
            }

            /**
             * 判断path是否是LeaderHostPath,
             * 即path是否为"/${jobName}/leader/election/host" ，且事件为节点删除
             *
             * @return
             */
            private boolean isLeaderCrashed() {
                return electionNode.isLeaderHostPath(path) && Type.NODE_REMOVED == event.getType();
            }

            /**
             * 判断给定路径是否从作业服务器禁用路径中移除
             * path是否以"/${jobName}/${ip}/disabled"开头，且事件为节点删除
             *
             * @return
             */
            private boolean isServerEnabled() {
                return serverNode.isLocalServerDisabledPath(path) && Type.NODE_REMOVED == event.getType();
            }

            /**
             * 判断给定路径是否从作业服务器暂停路径中移除
             * path是否以/${jobName}/servers/${ip}/paused开头，且事件为节点删除
             *
             * @return
             */
            private boolean isServerResumed() {
                return serverNode.isLocalJobPausedPath(path) && Type.NODE_REMOVED == event.getType();
            }

            boolean isServerOff() {
                return isServerDisabled() || isServerPaused() || isServerShutdown();
            }

            /**
             * 如果path被加入到禁用服务列表
             *
             * @return
             */
            private boolean isServerDisabled() {
                return serverNode.isLocalServerDisabledPath(path) && Type.NODE_ADDED == event.getType();
            }

            /**
             * 如果path被加入到暂停服务列表
             *
             * @return
             */
            private boolean isServerPaused() {
                return serverNode.isLocalJobPausedPath(path) && Type.NODE_ADDED == event.getType();
            }

            /**
             * 如果path被加入到shutdown列表
             *
             * @return
             */
            private boolean isServerShutdown() {
                return serverNode.isLocalJobShutdownPath(path) && Type.NODE_ADDED == event.getType();
            }
        }
    }
}
