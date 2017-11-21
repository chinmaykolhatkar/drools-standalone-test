import java.io.IOException;

import org.drools.core.RuleBaseConfiguration;
import org.drools.core.common.WorkingMemoryFactory;
import org.drools.core.event.DebugAgendaEventListener;
import org.drools.core.event.DebugRuleRuntimeEventListener;
import org.drools.core.reteoo.KieComponentFactory;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import com.datatorrent.cep.schema.Transaction;
import com.datatorrent.cep.transactionGenerator.TransactionGenerator;
import com.datatorrent.drools.DTWorkingMemoryFactory;
import com.datatorrent.drools.ObjectCache;
import com.datatorrent.drools.OnDemandKieSession;
import com.datatorrent.drools.SessionManager;

/**
 * Created by chinmay on 19/11/17.
 */
public class Perform1
{
  SessionManager sessionManager;
  KieSession[] sessions;
  TransactionGenerator gen;

  long ingestedCount = 0;

  // Configuration
  private int maxSessions = 1000;
  private final int cardCount = 1000;
  private final int ingestionRate = 1000;
  int maxIngestion = -1;

  public static void main(String[] args) throws IOException, InterruptedException
  {
    new Perform1().run();
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
        Object key = getKey(t);
        sessionManager.insert(key, t);
        sessionManager.fireAllFules(key);
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

      System.out.println("CurrentTime=" + System.currentTimeMillis() + ", " +
        "TimeTaken=" + timeTaken / 1000000L + ", " +
        "IngestedCount=" + ingestedCount + ", " +
        "AliveSessions=" + sessionManager.sessions.size() + ", " +
        "ParkedSessions=" + sessionManager.parkedResourceMap.size());
    }
  }

  private Object getKey(Transaction t)
  {
    return t.cardNumber % maxSessions;
  }

  private KieSession getSession(Transaction t)
  {
    int idx = (int)(t.cardNumber % maxSessions);
    return sessions[idx];
  }

  private void fini()
  {
    sessionManager.disposeAll();
  }

  private void init() throws IOException
  {
    ObjectCache.get().registerClass(Transaction.class, 2048, 100000);

    KieServices ks = KieServices.Factory.get();
    RuleBaseConfiguration conf = (RuleBaseConfiguration)ks.newKieBaseConfiguration();
    conf.setMaxThreads(10);
//    conf.setMultithreadEvaluation(true);
    conf.setComponentFactory(new KieComponentFactory() {
      @Override
      public WorkingMemoryFactory getWorkingMemoryFactory()
      {
        return new DTWorkingMemoryFactory();
      }
    });
    KieContainer kieContainer = ks.newKieClasspathContainer();
    KieBase kieBase = kieContainer.newKieBase(conf);

    sessionManager = new SessionManager(kieBase);

    gen = new TransactionGenerator();
    gen.setFraudTransactionPercentage(30);
    gen.setEnrichCustomers(true);
    gen.setEnrichPaymentCard(true);
    gen.setEnrichProduct(true);
    gen.setEnrichStorePOS(true);
    gen.setCardCount(cardCount);

    gen.generateData();
  }
}
