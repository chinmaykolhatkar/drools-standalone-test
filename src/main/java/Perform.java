import java.io.IOException;
import java.util.Map;

import org.drools.core.RuleBaseConfiguration;
import org.drools.core.common.DefaultAgenda;
import org.drools.core.common.WorkingMemoryFactory;
import org.drools.core.event.DebugAgendaEventListener;
import org.drools.core.event.DebugRuleRuntimeEventListener;
import org.drools.core.reteoo.KieComponentFactory;
import org.kie.api.KieBase;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.conf.MaxThreadsOption;

import com.datatorrent.cep.schema.Transaction;
import com.datatorrent.cep.transactionGenerator.TransactionGenerator;
import com.datatorrent.drools.DTWorkingMemoryFactory;
import com.datatorrent.drools.OnDemandKieSession;

/**
 * Created by chinmay on 19/11/17.
 */
public class Perform
{
  KieSession[] sessions;
  TransactionGenerator gen;
  long ingestedCount = 0;

  // Configuration
  private int maxSessions = 1;
  private final int cardCount = 1000;
  private final int ingestionRate = 100;
  int maxIngestion = -1;

  public static void main(String[] args) throws IOException, InterruptedException
  {
    new Perform().run();
  }

  public void run() throws IOException, InterruptedException
  {
    init();
    ingest();
    fini();
  }

  private void ingest() throws InterruptedException
  {
    while (true) {
      long startNano = System.nanoTime();

      for (int i=0; i < ingestionRate; ++i) {
        Transaction t = gen.generateTransaction(null);
        KieSession s = getSession(t);
        s.insert(t);
        ingestedCount++;
      }

      if ((maxIngestion > 0) && (ingestedCount > maxIngestion)) {
        break;
      }

      long timeTaken = System.nanoTime() - startNano;

      long remainingTime = (1000000000L - timeTaken) / 1000000L;
      if (remainingTime <= 0) {
        System.out.println("Took more time to ingest by " + -remainingTime + " ms");
      } else {
        Thread.sleep(remainingTime);
      }

      System.out.println("CurrentTime=" + System.currentTimeMillis() + ", TimeTaken=" + timeTaken / 1000000L + ", IngestedCount=" + ingestedCount);
    }
  }

  private KieSession getSession(Transaction t)
  {
    int idx = (int)(t.cardNumber % maxSessions);
    return sessions[idx];
  }

  private void fini()
  {
    for (KieSession session : sessions) {
      session.halt();
      session.dispose();
      session.destroy();
    }
  }

  private void init() throws IOException
  {
    System.setProperty("drools.jittingThreshold", "0");
    KieServices kieServices = KieServices.Factory.get();
    KieContainer kieContainer = kieServices.newKieClasspathContainer();

    gen = new TransactionGenerator();
    gen.setFraudTransactionPercentage(5);
    gen.setEnrichCustomers(true);
    gen.setEnrichPaymentCard(true);
    gen.setEnrichProduct(true);
    gen.setEnrichStorePOS(true);
    gen.setCardCount(cardCount);

    gen.generateData();

    maxSessions = Math.min(maxSessions, cardCount);
    sessions = new KieSession[maxSessions];

    for (int i=0; i<maxSessions; ++i) {
      final KieSession s = kieContainer.newKieSession();
//      s.addEventListener(new DebugRuleRuntimeEventListener());
//      s.addEventListener(new DebugAgendaEventListener());
      sessions[i] = s;
      new Thread("SessionFireRuleThread-" + i)
      {
        @Override
        public void run()
        {
          s.fireUntilHalt();
        }
      }.start();
    }
  }
}
