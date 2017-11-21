/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datatorrent.unsafe;

import com.google.common.base.Supplier;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class StupidPool<T>
{
  private final Supplier<T> generator;

  private final Queue<T> objects = new ConcurrentLinkedQueue<>();

  //note that this is just the max entries in the cache, pool can still create as many buffers as needed.
  private final int objectsCacheMaxCount;
  private final Class clazz;

  public StupidPool(
    Supplier<T> generator,
    Class clazz)
  {
    this.generator = generator;
    this.clazz = clazz;
    this.objectsCacheMaxCount = Integer.MAX_VALUE;
  }

  public StupidPool(
    Supplier<T> generator,
    int objectsCacheMaxCount,
    Class clazz)
  {
    this.generator = generator;
    this.objectsCacheMaxCount = objectsCacheMaxCount;
    this.clazz = clazz;
  }

  public ResourceHolder<T> take()
  {
    final T obj = objects.poll();
    return obj == null ? new ObjectResourceHolder(generator.get()) : new ObjectResourceHolder(obj);
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final T object;

    public ObjectResourceHolder(final T object)
    {
      this.object = object;
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call ObjectResourceHolder.close,
    // Then still use that object even though it will be offered to someone else in StupidPool.take
    @Override
    public T get()
    {
      if (closed.get()) {
        throw new RuntimeException("Already closed!");
      }

      return object;
    }

    public Class getClassObject()
    {
      return clazz;
    }

    @Override
    public void close() throws IOException
    {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      if (objects.size() < objectsCacheMaxCount) {
        if (!objects.offer(object)) {
        }
      } else {
      }
    }

    @Override
    protected void finalize() throws Throwable
    {
      try {
        if (!closed.get()) {
        }
      }
      finally {
        super.finalize();
      }
    }
  }
}
