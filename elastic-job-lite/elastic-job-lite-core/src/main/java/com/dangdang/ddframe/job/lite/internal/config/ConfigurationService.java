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

package com.dangdang.ddframe.job.lite.internal.config;

import com.dangdang.ddframe.job.exception.JobConfigurationException;
import com.dangdang.ddframe.job.exception.JobExecutionEnvironmentException;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.google.common.base.Optional;

/**
 * 弹性化分布式作业配置服务.
 * <p>
 * 向zookeeper存取{@link com.dangdang.ddframe.job.lite.config.LiteJobConfiguration}的服务类
 *
 * @author zhangliang
 * @author caohao
 */
public class ConfigurationService {

    /**
     * 节点的存储路径，及与zookeeper的交互类
     */
    private final JobNodeStorage jobNodeStorage;

    public ConfigurationService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
    }

    /**
     * 读取作业配置.
     * 因为jobNodeStorage已经存了jobName，所以它能够获取到对应的LiteJobConfiguration
     *
     * @param fromCache 是否从缓存中读取
     * @return 作业配置
     */
    public LiteJobConfiguration load(final boolean fromCache) {
        String result;
        if (fromCache) {
            result = jobNodeStorage.getJobNodeData(ConfigurationNode.ROOT);
            if (null == result) {
                result = jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT);
            }
        } else {
            result = jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT);
        }
        return LiteJobConfigurationGsonFactory.fromJson(result);
    }

    /**
     * 持久化分布式作业配置信息.
     *
     * @param liteJobConfig 作业配置
     */
    public void persist(final LiteJobConfiguration liteJobConfig) {
        checkConflictJob(liteJobConfig);
        // 只有当源节点不存在，或者本节点已经被修改，才去持久化
        if (!jobNodeStorage.isJobNodeExisted(ConfigurationNode.ROOT) || liteJobConfig.isOverwrite()) {
            jobNodeStorage.replaceJobNode(ConfigurationNode.ROOT, LiteJobConfigurationGsonFactory.toJson(liteJobConfig));
        }
    }

    private void checkConflictJob(final LiteJobConfiguration liteJobConfig) {
        Optional<LiteJobConfiguration> liteJobConfigFromZk = find();
        // 如果判断类型不一致就抛出异常
        if (liteJobConfigFromZk.isPresent() && !liteJobConfigFromZk.get().getTypeConfig().getJobClass().equals(liteJobConfig.getTypeConfig().getJobClass())) {
            throw new JobConfigurationException("Job conflict with register center. The job '%s' in register center's class is '%s', your job class is '%s'",
                    liteJobConfig.getJobName(), liteJobConfigFromZk.get().getTypeConfig().getJobClass(), liteJobConfig.getTypeConfig().getJobClass());
        }
    }

    private Optional<LiteJobConfiguration> find() {
        if (!jobNodeStorage.isJobNodeExisted(ConfigurationNode.ROOT)) {
            // 如果zk上尚未有注册节点，则返回一个空的Optional
            return Optional.absent();
        }
        LiteJobConfiguration result = LiteJobConfigurationGsonFactory.fromJson(jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT));
        if (null == result) {
            // TODO 应该删除整个job node,并非仅仅删除config node
            jobNodeStorage.removeJobNodeIfExisted(ConfigurationNode.ROOT);
        }
        return Optional.fromNullable(result);
    }

    /**
     * 检查本机与注册中心的时间误差秒数是否在允许范围.
     *
     * @throws JobExecutionEnvironmentException 本机与注册中心的时间误差秒数不在允许范围所抛出的异常
     */
    public void checkMaxTimeDiffSecondsTolerable() throws JobExecutionEnvironmentException {
        int maxTimeDiffSeconds = load(true).getMaxTimeDiffSeconds();
        if (-1 == maxTimeDiffSeconds) {
            return;
        }
        long timeDiff = Math.abs(System.currentTimeMillis() - jobNodeStorage.getRegistryCenterTime());
        if (timeDiff > maxTimeDiffSeconds * 1000L) {
            throw new JobExecutionEnvironmentException(
                    "Time different between job server and register center exceed '%s' seconds, max time different is '%s' seconds.", Long.valueOf(timeDiff / 1000).intValue(), maxTimeDiffSeconds);
        }
    }
}
