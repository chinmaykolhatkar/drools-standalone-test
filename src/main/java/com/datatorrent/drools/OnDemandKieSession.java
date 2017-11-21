package com.datatorrent.drools;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.drools.core.SessionConfiguration;
import org.drools.core.common.InternalAgenda;
import org.drools.core.common.InternalFactHandle;
import org.drools.core.event.AgendaEventSupport;
import org.drools.core.event.RuleEventListenerSupport;
import org.drools.core.event.RuleRuntimeEventSupport;
import org.drools.core.impl.InternalKnowledgeBase;
import org.drools.core.impl.StatefulKnowledgeSessionImpl;
import org.drools.core.spi.FactHandleFactory;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.event.rule.ObjectInsertedEvent;
import org.kie.api.event.rule.ObjectUpdatedEvent;
import org.kie.api.event.rule.RuleRuntimeEventListener;
import org.kie.api.runtime.Environment;
import org.kie.api.runtime.rule.FactHandle;

import com.datatorrent.unsafe.ResourceHolder;

/**
 * Created by chinmay on 20/11/17.
 */
public class OnDemandKieSession extends StatefulKnowledgeSessionImpl
{
  private long lastUpdateTs;

  private Map<Integer, ResourceHolder> rhMap = new LinkedHashMap<>();
  private ReadWriteLock lock = new ReentrantReadWriteLock();

  public long getLastUpdateTs()
  {
    return lastUpdateTs;
  }

  public OnDemandKieSession()
  {
    super();
  }

  public OnDemandKieSession(long id, InternalKnowledgeBase kBase)
  {
    super(id, kBase);
  }

  public OnDemandKieSession(long id, InternalKnowledgeBase kBase, boolean initInitFactHandle, SessionConfiguration config, Environment environment)
  {
    super(id, kBase, initInitFactHandle, config, environment);
  }

  public OnDemandKieSession(long id, InternalKnowledgeBase kBase, FactHandleFactory handleFactory, long propagationContext, SessionConfiguration config, InternalAgenda agenda, Environment environment)
  {
    super(id, kBase, handleFactory, propagationContext, config, agenda, environment);
  }

  public OnDemandKieSession(long id, InternalKnowledgeBase kBase, FactHandleFactory handleFactory, boolean initInitFactHandle, long propagationContext, SessionConfiguration config, Environment environment, RuleRuntimeEventSupport workingMemoryEventSupport, AgendaEventSupport agendaEventSupport, RuleEventListenerSupport ruleEventListenerSupport, InternalAgenda agenda)
  {
    super(id, kBase, handleFactory, initInitFactHandle, propagationContext, config, environment, workingMemoryEventSupport, agendaEventSupport, ruleEventListenerSupport, agenda);
  }

  @Override
  protected void init()
  {
    super.init();
    addEventListener(new RuleRuntimeEventListener()
    {
      @Override
      public void objectInserted(ObjectInsertedEvent event)
      {
        lock.readLock().lock();
        int id = ((InternalFactHandle)event.getFactHandle()).getId();
        ResourceHolder rh = ObjectCache.get().put(event.getObject());
        rhMap.put(id, rh);
        lock.readLock().unlock();
      }

      @Override
      public void objectUpdated(ObjectUpdatedEvent event)
      {
        lock.readLock().lock();
        int id = ((InternalFactHandle)event.getFactHandle()).getId();
        ObjectCache.get().replace(rhMap.get(id), event.getObject());
        lock.readLock().unlock();
      }

      @Override
      public void objectDeleted(ObjectDeletedEvent event)
      {
        lock.readLock().lock();
        int id = ((InternalFactHandle)event.getFactHandle()).getId();
        try {
          rhMap.get(id).close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        lock.readLock().unlock();
      }
    });
  }

  public FactHandle insert(ResourceHolder holder)
  {
    Object o = ObjectCache.get().get(holder);
    return insert(o);
  }

  @Override
  public FactHandle insert(Object object)
  {
    lock.readLock().lock();
    try {
      lastUpdateTs = System.currentTimeMillis();
      return super.insert(object);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int fireAllRules()
  {
    lock.readLock().lock();
    try {
      lastUpdateTs = System.currentTimeMillis();
      return super.fireAllRules();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void dispose()
  {
    if (lock.writeLock().tryLock()) {
      super.dispose();
      lock.writeLock().unlock();
    }
  }

  public Collection<ResourceHolder> getResourceHolder()
  {
    return rhMap.values();
  }

  public void registerClass(Class c)
  {
    ObjectCache.get().registerClass(c, 2000, 100000);
  }
}
