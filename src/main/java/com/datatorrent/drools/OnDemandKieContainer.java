package com.datatorrent.drools;

import org.drools.compiler.kie.builder.impl.KieContainerImpl;
import org.drools.compiler.kie.builder.impl.KieProject;
import org.kie.api.KieBase;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.ReleaseId;
import org.kie.api.runtime.KieSession;

/**
 * Created by chinmay on 20/11/17.
 */
public class OnDemandKieContainer extends KieContainerImpl
{
  public OnDemandKieContainer(KieProject kProject, KieRepository kr)
  {
    super(kProject, kr);
  }

  public OnDemandKieContainer(KieProject kProject, KieRepository kr, ReleaseId containerReleaseId)
  {
    super(kProject, kr, containerReleaseId);
  }

  public OnDemandKieContainer(String containerId, KieProject kProject, KieRepository kr)
  {
    super(containerId, kProject, kr);
  }

  public OnDemandKieContainer(String containerId, KieProject kProject, KieRepository kr, ReleaseId containerReleaseId)
  {
    super(containerId, kProject, kr, containerReleaseId);
  }

  @Override
  public KieSession newKieSession()
  {
    return super.newKieSession();
  }

  @Override
  public KieBase getKieBase()
  {
    return super.getKieBase();
  }
}
