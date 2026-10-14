/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * {@code FetchBuffer} buffers up {@link CompletedFetch the results} from the broker responses as they are received.
 * It is essentially a wrapper around a {@link java.util.Queue} of {@link CompletedFetch}. There is at most one
 * {@link CompletedFetch} per partition in the queue.
 *
 * <p/>
 *
 * <em>Note</em>: the queue of {@link CompletedFetch}es may be shared between the thread that adds the fetches (the
 * network thread in the case of the asynchronous consumer) and the thread that consumes them (the application
 * thread). The next-in-line fetch is only intended to be used from the consuming thread.
 */
public class FetchBuffer implements AutoCloseable {

    private final Logger log;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    private final ReentrantLock lock;
    private final Condition notEmptyCondition;

    private CompletedFetch nextInLineFetch;

    public FetchBuffer(final LogContext logContext) {
        this.log = logContext.logger(FetchBuffer.class);
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.lock = new ReentrantLock();
        this.notEmptyCondition = lock.newCondition();
    }

    /**
     * Returns {@code true} if there are no completed fetches pending to return to the user.
     *
     * @return {@code true} if the buffer is empty, {@code false} otherwise
     */
    boolean isEmpty() {
        lock.lock();

        try {
            return completedFetches.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return {@code true} if there are completed fetches that match the {@link Predicate}, {@code false} otherwise
     */
    boolean hasCompletedFetches(Predicate<CompletedFetch> predicate) {
        lock.lock();

        try {
            return completedFetches.stream().anyMatch(predicate);
        } finally {
            lock.unlock();
        }
    }

    void add(CompletedFetch completedFetch) {
        lock.lock();

        try {
            completedFetches.add(completedFetch);
            notEmptyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void addAll(Collection<CompletedFetch> completedFetches) {
        if (completedFetches == null || completedFetches.isEmpty())
            return;

        lock.lock();

        try {
            this.completedFetches.addAll(completedFetches);
            notEmptyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    CompletedFetch nextInLineFetch() {
        lock.lock();

        try {
            return nextInLineFetch;
        } finally {
            lock.unlock();
        }
    }

    void setNextInLineFetch(CompletedFetch nextInLineFetch) {
        lock.lock();

        try {
            this.nextInLineFetch = nextInLineFetch;
        } finally {
            lock.unlock();
        }
    }

    CompletedFetch peek() {
        lock.lock();

        try {
            return completedFetches.peek();
        } finally {
            lock.unlock();
        }
    }

    CompletedFetch poll() {
        lock.lock();

        try {
            return completedFetches.poll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Allows the caller to await presence of data in the buffer. The method will block, returning only
     * under one of the following conditions:
     *
     * <ol>
     *     <li>The buffer was already non-empty on entry</li>
     *     <li>The buffer was populated during the wait</li>
     *     <li>The remaining time on the {@link Timer timer} elapsed</li>
     *     <li>The thread was interrupted</li>
     * </ol>
     *
     * @param timer Timer that provides time to wait
     * @throws InterruptException if the thread is interrupted while waiting
     */
    void awaitNotEmpty(Timer timer) {
        lock.lock();

        try {
            while (completedFetches.isEmpty()) {
                // Update the timer before we head into the loop in case it took a while to get the lock.
                timer.update();

                if (timer.isExpired())
                    break;

                if (!notEmptyCondition.await(timer.remainingMs(), TimeUnit.MILLISECONDS))
                    break;
            }
        } catch (InterruptedException e) {
            throw new InterruptException("Timeout waiting for results from fetching records", e);
        } finally {
            lock.unlock();
            timer.update();
        }
    }

    /**
     * Updates the buffer to retain only the fetch data that corresponds to the given partitions. Any previously
     * {@link CompletedFetch fetched data} is removed if its partition is not in the given set of partitions.
     *
     * @param partitions {@link Set} of {@link TopicPartition}s for which any buffered data should be kept
     */
    void retainAll(final Set<TopicPartition> partitions) {
        lock.lock();

        try {
            completedFetches.removeIf(cf -> maybeDrain(partitions, cf));

            if (maybeDrain(partitions, nextInLineFetch))
                nextInLineFetch = null;
        } finally {
            lock.unlock();
        }
    }

    private boolean maybeDrain(final Set<TopicPartition> partitions, final CompletedFetch completedFetch) {
        if (completedFetch != null && !partitions.contains(completedFetch.partition)) {
            log.debug("Removing {} from buffered fetch data as it is not in the set of partitions to retain ({})", completedFetch.partition, partitions);
            completedFetch.drain();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Return the set of {@link TopicPartition partitions} for which we have data in the buffer.
     *
     * @return {@link TopicPartition Partition} set
     */
    Set<TopicPartition> bufferedPartitions() {
        lock.lock();

        try {
            final Set<TopicPartition> partitions = new HashSet<>();

            if (nextInLineFetch != null && !nextInLineFetch.isConsumed()) {
                partitions.add(nextInLineFetch.partition);
            }

            completedFetches.forEach(cf -> partitions.add(cf.partition));
            return partitions;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        idempotentCloser.close(() -> {
            log.debug("Closing the fetch buffer");

            lock.lock();

            try {
                if (nextInLineFetch != null) {
                    nextInLineFetch.drain();
                    nextInLineFetch = null;
                }

                completedFetches.forEach(CompletedFetch::drain);
                completedFetches.clear();
            } finally {
                lock.unlock();
            }
        }, () -> log.warn("The fetch buffer was previously closed"));
    }
}
