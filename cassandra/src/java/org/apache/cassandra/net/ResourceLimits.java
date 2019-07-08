/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

abstract class ResourceLimits
{
    /**
     * Represents permits to utilise a resource and ways to allocate and release them.
     *
     * Two implementations are currently provided:
     * 1. {@link Concurrent}, for shared limits, which is thread-safe;
     * 2. {@link Basic}, for limits that are not shared between threads, is not thread-safe.
     */
    interface Limit
    {
        /**
         * @return total amount of permits represented by this {@link Limit} - the capacity
         */
        long limit();

        /**
         * @return remaining, unallocated permit amount
         */
        long remaining();

        /**
         * @return amount of permits currently in use
         */
        long using();

        /**
         * Attempts to allocate an amount of permits from this limit. If allocated, <em>MUST</em> eventually
         * be released back with {@link #release(long)}.
         *
         * @return {@code true} if the allocation was successful, {@code false} otherwise
         */
        boolean tryAllocate(long amount);

        /**
         * @param amount return the amount of permits back to this limit
         */
        void release(long amount);
    }

    /**
     * A thread-safe permit container.
     */
    public static class Concurrent implements Limit
    {
        private final long limit;

        private volatile long using;
        private static final AtomicLongFieldUpdater<Concurrent> usingUpdater =
            AtomicLongFieldUpdater.newUpdater(Concurrent.class, "using");

        Concurrent(long limit)
        {
            this.limit = limit;
        }

        public long limit()
        {
            return limit;
        }

        public long remaining()
        {
            return limit - using;
        }

        public long using()
        {
            return using;
        }

        public boolean tryAllocate(long amount)
        {
            long current, next;
            do
            {
                current = using;
                next = current + amount;

                if (next > limit)
                    return false;
            }
            while (!usingUpdater.compareAndSet(this, current, next));

            return true;
        }

        public void release(long amount)
        {
            assert amount >= 0;
            long using = usingUpdater.addAndGet(this, -amount);
            assert using >= 0;
        }
    }

    /**
     * A cheaper, thread-unsafe permit container to be used for unshared limits.
     */
    static class Basic implements Limit
    {
        private final long limit;
        private long using;

        Basic(long limit)
        {
            this.limit = limit;
        }

        public long limit()
        {
            return limit;
        }

        public long remaining()
        {
            return limit - using;
        }

        public long using()
        {
            return using;
        }

        public boolean tryAllocate(long amount)
        {
            if (using + amount > limit)
                return false;

            using += amount;
            return true;
        }

        public void release(long amount)
        {
            assert amount >= 0 && amount <= using;
            using -= amount;
        }
    }

    /**
     * A convenience class that groups a per-endpoint limit with the global one
     * to allow allocating/releasing permits from/to both limits as one logical operation.
     */
    static class EndpointAndGlobal
    {
        final Limit endpoint;
        final Limit global;

        EndpointAndGlobal(Limit endpoint, Limit global)
        {
            this.endpoint = endpoint;
            this.global = global;
        }

        /**
         * @return {@code INSUFFICIENT_GLOBAL} if there weren't enough permits in the global limit, or
         *         {@code INSUFFICIENT_ENDPOINT} if there weren't enough permits in the per-endpoint limit, or
         *         {@code SUCCESS} if there were enough permits to take from both.
         */
        Outcome tryAllocate(long amount)
        {
            if (!global.tryAllocate(amount))
                return Outcome.INSUFFICIENT_GLOBAL;

            if (endpoint.tryAllocate(amount))
                return Outcome.SUCCESS;

            global.release(amount);
            return Outcome.INSUFFICIENT_ENDPOINT;
        }

        void release(long amount)
        {
            endpoint.release(amount);
            global.release(amount);
        }
    }

    enum Outcome { SUCCESS, INSUFFICIENT_ENDPOINT, INSUFFICIENT_GLOBAL }
}
