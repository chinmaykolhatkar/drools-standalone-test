package com.datatorrent.drools;

import org.drools.core.SessionConfiguration;
import org.drools.core.common.InternalAgenda;
import org.drools.core.common.InternalFactHandle;
import org.drools.core.common.InternalWorkingMemory;
import org.drools.core.common.PhreakWorkingMemoryFactory;
import org.drools.core.event.AgendaEventSupport;
import org.drools.core.event.RuleEventListenerSupport;
import org.drools.core.event.RuleRuntimeEventSupport;
import org.drools.core.impl.InternalKnowledgeBase;
import org.drools.core.impl.StatefulKnowledgeSessionImpl;
import org.drools.core.spi.FactHandleFactory;
import org.kie.api.runtime.Environment;

/**
 * Created by chinmay on 20/11/17.
 */
public class DTWorkingMemoryFactory extends PhreakWorkingMemoryFactory
{
  @Override
  public InternalWorkingMemory createWorkingMemory(long id, InternalKnowledgeBase kBase, SessionConfiguration config, Environment environment)
  {
    OnDemandKieSession cachedWm = (OnDemandKieSession)kBase.getCachedSession(config, environment);
    return cachedWm != null ? cachedWm : new OnDemandKieSession(id, kBase, true, config, environment);
  }

  @Override
  public InternalWorkingMemory createWorkingMemory(long id, InternalKnowledgeBase kBase, FactHandleFactory handleFactory, long propagationContext, SessionConfiguration config, InternalAgenda agenda, Environment environment)
  {
    return new OnDemandKieSession(id, kBase, handleFactory, propagationContext, config, agenda, environment);
  }

  @Override
  public InternalWorkingMemory createWorkingMemory(long id, InternalKnowledgeBase kBase, FactHandleFactory handleFactory, InternalFactHandle initialFactHandle, long propagationContext, SessionConfiguration config, Environment environment, RuleRuntimeEventSupport workingMemoryEventSupport, AgendaEventSupport agendaEventSupport, RuleEventListenerSupport ruleEventListenerSupport, InternalAgenda agenda)
  {
    return new OnDemandKieSession(id, kBase, handleFactory, true, propagationContext, config, environment, workingMemoryEventSupport, agendaEventSupport, ruleEventListenerSupport, agenda);
  }
}
