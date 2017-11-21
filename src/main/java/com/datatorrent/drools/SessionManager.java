package com.datatorrent.drools;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;

import com.datatorrent.unsafe.ResourceHolder;

/**
 * Created by chinmay on 20/11/17.
 */
public class SessionManager
{
  private final long DISPOSE_AFTER = 5000;
  private KieBase base;

  public Map<Object, OnDemandKieSession> sessions = new ConcurrentHashMap<>();
  private Timer timer;
  public Map<Object, Collection<ResourceHolder>> parkedResourceMap = new HashMap<>();

  public SessionManager(KieBase base)
  {
    this.base = base;
    this.timer = new Timer(true);
    timer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        Iterator<Map.Entry<Object, OnDemandKieSession>> it = sessions.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<Object, OnDemandKieSession> next = it.next();
          Object key = next.getKey();
          OnDemandKieSession s = next.getValue();
          if ((System.currentTimeMillis() - s.getLastUpdateTs()) > DISPOSE_AFTER) {
            s.dispose();
            parkedResourceMap.put(key, s.getResourceHolder());
            it.remove();
          }
        }
      }
    }, 0, 1000);
  }

  public void insert(Object key, Object o)
  {
    KieSession s = getSession(key);
    s.insert(o);
  }

  public void fireAllFules(Object key)
  {
    KieSession s = getSession(key);
    s.fireAllRules();
  }

  public void disposeAll()
  {
    Iterator<Map.Entry<Object, OnDemandKieSession>> it = sessions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Object, OnDemandKieSession> next = it.next();
      Object key = next.getKey();
      OnDemandKieSession s = next.getValue();
      s.dispose();
      s.destroy();
      parkedResourceMap.remove(key);
      it.remove();
    }
  }

  private KieSession getSession(Object key)
  {
    OnDemandKieSession s = sessions.get(key);
    if (s == null) {
      s = (OnDemandKieSession) base.newKieSession();
      sessions.put(key, s);
      Collection<ResourceHolder> resourceHolders = parkedResourceMap.get(key);
      if (resourceHolders != null) {
        for (ResourceHolder resourceHolder : resourceHolders) {
          s.insert(resourceHolder);
        }
        parkedResourceMap.remove(key);
      }
    }

    return s;
  }

}
