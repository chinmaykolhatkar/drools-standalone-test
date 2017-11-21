package com.datatorrent.drools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Supplier;

import com.datatorrent.unsafe.ResourceHolder;
import com.datatorrent.unsafe.StupidPool;

/**
 * Created by chinmay on 20/11/17.
 */
public class ObjectCache
{
  private Map<Class, StupidPool> registeredClasses = new HashMap<>();

  private Kryo k = new Kryo();

  private static ObjectCache cache = new ObjectCache();

  private ObjectCache()
  {
  }

  public static ObjectCache get()
  {
    return cache;
  }

  public void registerClass(Class c, final int size, int eagerLoadCount)
  {
    if (registeredClasses.containsKey(c)) {
      return;
    }

    StupidPool<ByteBuffer> pool = new StupidPool<>(new Supplier<ByteBuffer>()
    {
      @Override
      public ByteBuffer get()
      {
        return ByteBuffer.allocateDirect(size);
      }
    }, c);

    registeredClasses.put(c, pool);
    List<ResourceHolder> tempHolder = new ArrayList<>();
    for (int i = 0; i < eagerLoadCount; ++i) {
      tempHolder.add(pool.take());
    }
    for (ResourceHolder resourceHolder : tempHolder) {
      try {
        resourceHolder.close();
      } catch (IOException e) {
        throw new RuntimeException("Unable to return object to pool");
      }
    }
  }

  public ResourceHolder<ByteBuffer> replace(ResourceHolder<ByteBuffer> rh, Object newO)
  {
    byte[] ser;
    try {
      ser = ser(newO);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    ByteBuffer bb = rh.get();
    bb.clear();
    bb.put(ser);
    return rh;
  }

  public ResourceHolder put(Object o)
  {
    StupidPool pool = registeredClasses.get(o.getClass());

    byte[] ser;
    try {
      ser = ser(o);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    ResourceHolder<ByteBuffer> take = pool.take();
    ByteBuffer bb = take.get();
    bb.put(ser);
    return take;
  }

  public Object get(ResourceHolder rh)
  {
    return de(rh);
  }

  private byte[] ser(Object t1) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    k.writeObject(output, t1);
    output.close();

    return bos.toByteArray();
  }

  private Object de(ResourceHolder<ByteBuffer> rh)
  {
    ByteBuffer bb = rh.get();
    byte[] b = new byte[bb.capacity()];
    bb.rewind();
    bb.get(b, 0, b.length);
    try (Input i = new Input(b)) {
      return k.readObject(i, rh.getClassObject());
    }
  }
}
