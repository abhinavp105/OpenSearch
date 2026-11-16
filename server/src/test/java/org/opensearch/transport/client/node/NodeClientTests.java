/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.client.node;

import org.opensearch.action.ActionModule.DynamicActionRegistry;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchAdmission;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskListener;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class NodeClientTests extends OpenSearchTestCase {
    private TestThreadPool threadPool;
    private NodeClient client;
    private final AtomicReference<TaskId> observedParent = new AtomicReference<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        client = new NodeClient(Settings.EMPTY, threadPool);
        TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        DynamicActionRegistry registry = new DynamicActionRegistry();
        registerAction(registry, taskManager, SearchAction.INSTANCE);
        registerAction(registry, taskManager, GetAction.INSTANCE);
        client.initialize(registry, () -> "local", null, new NamedWriteableRegistry(List.of()));
    }

    private <Response extends ActionResponse> void registerAction(
        DynamicActionRegistry registry,
        TaskManager taskManager,
        ActionType<Response> action
    ) {
        registry.registerDynamicAction(
            action,
            new TransportAction<ActionRequest, Response>(action.name(), new ActionFilters(Set.of()), taskManager) {
                @Override
                protected void doExecute(Task task, ActionRequest request, ActionListener<Response> listener) {
                    observedParent.set(task.getParentTaskId());
                    listener.onResponse(null);
                }
            }
        );
    }

    @Override
    public void tearDown() throws Exception {
        try {
            client.close();
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        } finally {
            super.tearDown();
        }
    }

    public void testSearchInheritsRewriteParentBeforeTaskRegistration() {
        TaskId parent = new TaskId("local", 123);
        threadPool.getThreadContext().putTransient(SearchAdmission.REWRITE_PARENT_TASK_ID, parent);
        for (boolean taskListener : new boolean[] { false, true }) {
            assertParent(SearchAction.INSTANCE, new SearchRequest(), parent, taskListener);
        }
    }

    public void testExplicitSearchParentTakesPrecedence() {
        threadPool.getThreadContext().putTransient(SearchAdmission.REWRITE_PARENT_TASK_ID, new TaskId("local", 123));
        TaskId explicitParent = new TaskId("other", 456);
        for (boolean taskListener : new boolean[] { false, true }) {
            SearchRequest request = new SearchRequest();
            request.setParentTask(explicitParent);
            assertParent(SearchAction.INSTANCE, request, explicitParent, taskListener);
        }
    }

    public void testIndependentSearchDoesNotInheritRewriteParent() {
        for (boolean taskListener : new boolean[] { false, true }) {
            try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().newStoredContext(true)) {
                TaskId parent = new TaskId("local", 123);
                threadPool.getThreadContext().putTransient(SearchAdmission.REWRITE_PARENT_TASK_ID, parent);
                assertParent(SearchAction.INSTANCE, new SearchRequest(), parent, taskListener);
            }
            assertParent(SearchAction.INSTANCE, new SearchRequest(), TaskId.EMPTY_TASK_ID, taskListener);
        }
    }

    public void testNonSearchRequestDoesNotInheritRewriteParent() {
        threadPool.getThreadContext().putTransient(SearchAdmission.REWRITE_PARENT_TASK_ID, new TaskId("local", 123));
        for (boolean taskListener : new boolean[] { false, true }) {
            assertParent(GetAction.INSTANCE, new GetRequest("index", "1"), TaskId.EMPTY_TASK_ID, taskListener);
        }
    }

    private <Response extends ActionResponse> void assertParent(
        ActionType<Response> action,
        ActionRequest request,
        TaskId expectedParent,
        boolean taskListener
    ) {
        observedParent.set(null);
        AtomicBoolean completed = new AtomicBoolean();
        if (taskListener) {
            client.executeLocally(action, request, new TaskListener<Response>() {
                @Override
                public void onResponse(Task task, Response response) {
                    assertEquals(expectedParent, task.getParentTaskId());
                    completed.set(true);
                }

                @Override
                public void onFailure(Task task, Exception e) {
                    throw new AssertionError(e);
                }
            });
        } else {
            client.executeLocally(
                action,
                request,
                ActionListener.wrap(response -> completed.set(true), e -> { throw new AssertionError(e); })
            );
        }
        assertTrue(completed.get());
        assertEquals(expectedParent, observedParent.get());
        assertEquals(expectedParent, request.getParentTask());
    }
}
