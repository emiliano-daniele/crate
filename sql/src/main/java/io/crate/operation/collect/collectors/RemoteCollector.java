/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect.collectors;

import com.google.common.annotations.VisibleForTesting;
import io.crate.action.job.JobRequest;
import io.crate.action.job.JobResponse;
import io.crate.action.job.TransportJobAction;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.PageDownstreamContext;
import io.crate.metadata.Routing;
import io.crate.operation.NodeOperation;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projections;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RemoteCollector implements CrateCollector {

    private static final Logger LOGGER = Loggers.getLogger(RemoteCollector.class);
    private static final int SENDER_PHASE_ID = 0;
    private static final int RECEIVER_PHASE_ID = 1;

    private final UUID jobId;
    private final String localNode;
    private final TransportJobAction transportJobAction;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final JobContextService jobContextService;
    private final RamAccountingContext ramAccountingContext;
    private final RowConsumer consumer;

    private final Object killLock = new Object();
    private final boolean scrollRequired;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final int shardId;
    private final String indexName;
    private final RoutedCollectPhase originalCollectPhase;
    private JobExecutionContext context = null;
    private boolean collectorKilled = false;

    public RemoteCollector(UUID jobId,
                           String indexName,
                           int shardId,
                           TransportJobAction transportJobAction,
                           TransportKillJobsNodeAction transportKillJobsNodeAction,
                           JobContextService jobContextService,
                           RamAccountingContext ramAccountingContext,
                           RowConsumer consumer,
                           RoutedCollectPhase originalCollectPhase,
                           ClusterService clusterService,
                           ThreadPool threadPool) {
        this.jobId = jobId;
        this.indexName = indexName;
        this.shardId = shardId;
        this.scrollRequired = consumer.requiresScroll();
        this.transportJobAction = transportJobAction;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        this.jobContextService = jobContextService;
        this.ramAccountingContext = ramAccountingContext;
        this.consumer = consumer;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.originalCollectPhase = originalCollectPhase;
        this.localNode = clusterService.localNode().getId();
    }

    @Override
    public void doCollect() {
        IndexShardRoutingTable shardRoutings = clusterService.state().routingTable().shardRoutingTable(indexName, shardId);
        // for update operations primaryShards must be used
        // (for others that wouldn't be the case, but at this point it is not easily visible which is the case)
        ShardRouting shardRouting = shardRoutings.primaryShard();
        if (shardRouting.started() == false) {
            LOGGER.warn("Unable to collect from a shard that's not started. Waiting for primary shard {} to start.",
                shardRouting.shardId());

            ClusterStateObserver clusterStateObserver = new ClusterStateObserver(clusterService, LOGGER, threadPool.getThreadContext());
            clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    doCollect();
                }

                @Override
                public void onClusterServiceClose() {

                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // There can, in theory, be a case where we discovered the primary as not being started, [it became
                    // started] we added this state observer and we didn't get any other cluster state update. So our
                    // observer didn't catch the shard state update that came right before we registered it. So we check
                    // the status of the shard we're interested in one more time and if it's indeed still not started
                    // we fail.
                    ShardRouting primaryShardRouting =
                        clusterService.state().routingTable().shardRoutingTable(indexName, shardId).primaryShard();

                    if (primaryShardRouting.started()) {
                        startRemoteCollector(primaryShardRouting);
                    } else {
                        // TODO: log error and fail operation (hm, kill? so it can't ever start)
                    }
                }

            }, newState -> newState.routingTable().shardRoutingTable(indexName, shardId).primaryShard().started());
            return;
        }

        startRemoteCollector(shardRouting);
    }

    private void startRemoteCollector(ShardRouting shardRouting) {
        String remoteNode = shardRouting.currentNodeId();
        assert remoteNode != null : "primaryShard not assigned :(";
        RoutedCollectPhase newCollectPhase =
            createRemoteCollectPhase(jobId, originalCollectPhase, indexName, shardId, remoteNode);

        if (!createLocalContext(newCollectPhase)) {
            return;
        }
        createRemoteContext(newCollectPhase, remoteNode);
    }

    @VisibleForTesting
    boolean createLocalContext(RoutedCollectPhase collectPhase) {
        JobExecutionContext.Builder builder = createPageDownstreamContext(collectPhase);
        try {
            synchronized (killLock) {
                if (collectorKilled) {
                    consumer.accept(null, new InterruptedException());
                    return false;
                }
                context = jobContextService.createContext(builder);
                context.start();
                return true;
            }
        } catch (Throwable t) {
            if (context == null) {
                consumer.accept(null, t);
            } else {
                context.kill();
            }
            return false;
        }
    }

    @VisibleForTesting
    void createRemoteContext(RoutedCollectPhase collectPhase, String remoteNode) {

        NodeOperation nodeOperation = new NodeOperation(
            collectPhase, Collections.singletonList(localNode), RECEIVER_PHASE_ID, (byte) 0);

        synchronized (killLock) {
            if (collectorKilled) {
                context.kill();
                return;
            }
            transportJobAction.execute(
                remoteNode,
                new JobRequest(jobId, localNode, Collections.singletonList(nodeOperation)),
                new ActionListener<JobResponse>() {
                    @Override
                    public void onResponse(JobResponse jobResponse) {
                        LOGGER.trace("RemoteCollector jobAction=onResponse");
                        if (collectorKilled) {
                            killRemoteContext();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.error("RemoteCollector jobAction=onFailure", e);
                        context.kill();
                    }
                }
            );
        }
    }

    private RoutedCollectPhase createRemoteCollectPhase(
        UUID childJobId, RoutedCollectPhase collectPhase, String index, Integer shardId, String nodeId) {

        Routing routing = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder().put(nodeId,
            TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(index, Collections.singletonList(shardId)).map()).map());
        return new RoutedCollectPhase(
            childJobId,
            SENDER_PHASE_ID,
            collectPhase.name(),
            routing,
            collectPhase.maxRowGranularity(),
            collectPhase.toCollect(),
            new ArrayList<>(Projections.shardProjections(collectPhase.projections())),
            collectPhase.whereClause(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
    }

    private JobExecutionContext.Builder createPageDownstreamContext(RoutedCollectPhase routedCollectPhase) {
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId, localNode);

        PassThroughPagingIterator<Integer, Row> pagingIterator;
        if (scrollRequired) {
            pagingIterator = PassThroughPagingIterator.repeatable();
        } else {
            pagingIterator = PassThroughPagingIterator.oneShot();
        }
        builder.addSubContext(new PageDownstreamContext(
            LOGGER,
            localNode,
            RECEIVER_PHASE_ID,
            "remoteCollectReceiver",
            consumer,
            pagingIterator,
            DataTypes.getStreamers(routedCollectPhase.outputTypes()),
            ramAccountingContext,
            1
        ));
        return builder;
    }

    private void killRemoteContext() {
        transportKillJobsNodeAction.broadcast(new KillJobsRequest(Collections.singletonList(jobId)),
            new ActionListener<Long>() {

                @Override
                public void onResponse(Long numKilled) {
                    context.kill();
                }

                @Override
                public void onFailure(Exception e) {
                    context.kill();
                }
            });
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        synchronized (killLock) {
            collectorKilled = true;
            /**
             * due to the lock there are 3 kill windows:
             *
             *  1. localContext not even created - doCollect aborts
             *  2. localContext created, no requests sent - doCollect aborts
             *  3. localContext created, requests sent - clean-up happens once response from remote is received
             */
        }
    }
}
