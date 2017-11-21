package com.datatorrent.drools;

import org.drools.core.impl.KnowledgeBaseImpl;
import org.kie.api.runtime.Environment;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;

/**
 * Created by chinmay on 20/11/17.
 */
public class OnDemandKnowledgeBase extends KnowledgeBaseImpl
{
  @Override
  public KieSession newKieSession(KieSessionConfiguration conf, Environment environment)
  {
    return super.newKieSession(conf, environment);
  }
}
