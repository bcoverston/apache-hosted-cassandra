/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.triggers;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.TriggerDefinition;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class TriggerExecutor
{
    public static final TriggerExecutor instance = new TriggerExecutor();

    private final Map<String, ITrigger> cachedTriggers = Maps.newConcurrentMap();
    private final ClassLoader parent = Thread.currentThread().getContextClassLoader();
    private volatile ClassLoader customClassLoader;

    private TriggerExecutor()
    {
        reloadClasses();
    }

    /**
     * Reload the triggers which is already loaded, Invoking this will update
     * the class loader so new jars can be loaded.
     */
    public void reloadClasses()
    {
        File triggerDirectory = FBUtilities.cassandraTriggerDir();
        if (triggerDirectory == null)
            return;
        customClassLoader = new CustomClassLoader(parent, triggerDirectory);
        cachedTriggers.clear();
    }

    public PartitionUpdate execute(PartitionUpdate updates) throws InvalidRequestException
    {
        List<Mutation> intermediate = executeInternal(updates);
        if (intermediate == null || intermediate.isEmpty())
            return updates;

        return PartitionUpdate.merge(validateForSinglePartition(updates.metadata().cfId, updates.partitionKey(), intermediate));
    }

    public Collection<Mutation> execute(Collection<? extends IMutation> mutations) throws InvalidRequestException
    {
        boolean hasCounters = false;
        List<Mutation> augmentedMutations = null;

        for (IMutation mutation : mutations)
        {
            if (mutation instanceof CounterMutation)
                hasCounters = true;

            for (PartitionUpdate upd : mutation.getPartitionUpdates())
            {
                List<Mutation> augmentations = executeInternal(upd);
                if (augmentations == null || augmentations.isEmpty())
                    continue;

                validate(augmentations);

                if (augmentedMutations == null)
                    augmentedMutations = new LinkedList<>();
                augmentedMutations.addAll(augmentations);
            }
        }

        if (augmentedMutations == null)
            return null;

        if (hasCounters)
            throw new InvalidRequestException("Counter mutations and trigger mutations cannot be applied together atomically.");

        @SuppressWarnings("unchecked")
        Collection<Mutation> originalMutations = (Collection<Mutation>) mutations;

        return mergeMutations(Iterables.concat(originalMutations, augmentedMutations));
    }

    private Collection<Mutation> mergeMutations(Iterable<Mutation> mutations)
    {
        ListMultimap<Pair<String, ByteBuffer>, Mutation> groupedMutations = ArrayListMultimap.create();

        for (Mutation mutation : mutations)
        {
            Pair<String, ByteBuffer> key = Pair.create(mutation.getKeyspaceName(), mutation.key().getKey());
            groupedMutations.put(key, mutation);
        }

        List<Mutation> merged = new ArrayList<>(groupedMutations.size());
        for (Pair<String, ByteBuffer> key : groupedMutations.keySet())
            merged.add(Mutation.merge(groupedMutations.get(key)));

        return merged;
    }

    private Collection<PartitionUpdate> validateForSinglePartition(UUID cfId,
                                                                   DecoratedKey key,
                                                                   Collection<Mutation> tmutations)
    throws InvalidRequestException
    {
        validate(tmutations);

        if (tmutations.size() == 1)
        {
            Collection<PartitionUpdate> updates = Iterables.getOnlyElement(tmutations).getPartitionUpdates();
            if (updates.size() > 1)
                throw new InvalidRequestException("The updates generated by triggers are not all for the same partition");
            validateSamePartition(cfId, key, Iterables.getOnlyElement(updates));
            return updates;
        }

        ArrayList<PartitionUpdate> updates = new ArrayList<>(tmutations.size());
        for (Mutation mutation : tmutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                validateSamePartition(cfId, key, update);
                updates.add(update);
            }
        }
        return updates;
    }

    private void validateSamePartition(UUID cfId, DecoratedKey key, PartitionUpdate update)
    throws InvalidRequestException
    {
        if (!key.equals(update.partitionKey()))
            throw new InvalidRequestException("Partition key of additional mutation does not match primary update key");

        if (!cfId.equals(update.metadata().cfId))
            throw new InvalidRequestException("table of additional mutation does not match primary update table");
    }

    private void validate(Collection<Mutation> tmutations) throws InvalidRequestException
    {
        for (Mutation mutation : tmutations)
        {
            QueryProcessor.validateKey(mutation.key().getKey());
            for (PartitionUpdate update : mutation.getPartitionUpdates())
                update.validate();
        }
    }

    /**
     * Switch class loader before using the triggers for the column family, if
     * not loaded them with the custom class loader.
     */
    private List<Mutation> executeInternal(PartitionUpdate update)
    {
        Map<String, TriggerDefinition> triggers = update.metadata().getTriggers();
        if (triggers.isEmpty())
            return null;
        List<Mutation> tmutations = Lists.newLinkedList();
        Thread.currentThread().setContextClassLoader(customClassLoader);
        try
        {
            for (TriggerDefinition td : triggers.values())
            {
                ITrigger trigger = cachedTriggers.get(td.classOption);
                if (trigger == null)
                {
                    trigger = loadTriggerInstance(td.classOption);
                    cachedTriggers.put(td.classOption, trigger);
                }
                Collection<Mutation> temp = trigger.augment(update);
                if (temp != null)
                    tmutations.addAll(temp);
            }
            return tmutations;
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Exception while creating trigger on table with ID: %s", update.metadata().cfId), ex);
        }
        finally
        {
            Thread.currentThread().setContextClassLoader(parent);
        }
    }

    public synchronized ITrigger loadTriggerInstance(String triggerName) throws Exception
    {
        // double check.
        if (cachedTriggers.get(triggerName) != null)
            return cachedTriggers.get(triggerName);
        return (ITrigger) customClassLoader.loadClass(triggerName).getConstructor().newInstance();
    }
}
