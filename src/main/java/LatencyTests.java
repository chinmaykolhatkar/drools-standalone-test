
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;


import org.kie.api.KieServices;
import org.kie.api.event.rule.AfterMatchFiredEvent;
import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.event.rule.AgendaGroupPoppedEvent;
import org.kie.api.event.rule.AgendaGroupPushedEvent;
import org.kie.api.event.rule.BeforeMatchFiredEvent;
import org.kie.api.event.rule.MatchCancelledEvent;
import org.kie.api.event.rule.MatchCreatedEvent;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.event.rule.ObjectInsertedEvent;
import org.kie.api.event.rule.ObjectUpdatedEvent;
import org.kie.api.event.rule.RuleFlowGroupActivatedEvent;
import org.kie.api.event.rule.RuleFlowGroupDeactivatedEvent;
import org.kie.api.event.rule.RuleRuntimeEventListener;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.datatorrent.cep.schema.Transaction;
import com.datatorrent.cep.transactionGenerator.TransactionGenerator;

/**
 * Created by ambarish on 25/10/17.
 */
public class LatencyTests
{

  public static void main(String args[])
  {
    Logger.getRootLogger().setLevel(Level.OFF);
    int numTransactions = Integer.parseInt(System.getProperty("n"));
    int numSessions = Integer.parseInt(System.getProperty("s"));

    Runtime runtime = Runtime.getRuntime();
    long beforeUsedMem = runtime.totalMemory() - runtime.freeMemory();
    HashMap<Integer, KieSession> sessions = new HashMap<Integer, KieSession>(numSessions);

    TransactionGenerator gen = getTransactionGenerator();

    try {
      gen.generateData();
    } catch (IOException e) {
      e.printStackTrace();
    }

    KieServices kieServices = KieServices.Factory.get();
    KieContainer kieContainer = kieServices.newKieClasspathContainer();
    KieSession kieSession = null;

    for (int i = 0; i < numTransactions; i++) {
      if(i%1000==0){
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      Transaction t = gen.generateTransaction(null);
      int sid = t.getCustomer().hashCode() % numSessions;

      if (!sessions.containsKey(sid)) {
        kieSession = kieContainer.newKieSession();
        sessions.put(sid, kieSession);
      }
      kieSession = sessions.get(sid);

      kieSession.insert(t);
      kieSession.fireAllRules();
    }

    long afterUsedMem = runtime.totalMemory() - runtime.freeMemory();
//    System.out.println(numSessions + "," + numTransactions + "," + (double)((double)(afterUsedMem - beforeUsedMem)
//      /1000000000));
    try {
      PrintWriter pw = new PrintWriter(new FileOutputStream(
        new File("log.txt"),
        true /* append = true */));
      pw.append(numSessions + "," + numTransactions + "," + (double)((double)(afterUsedMem - beforeUsedMem)
        /1000000000) + "\n");

      pw.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

//    kieSession.dispose();
//    kieSession.destroy();

  }

  public static TransactionGenerator getTransactionGenerator()
  {
    TransactionGenerator gen = new TransactionGenerator();
    gen.setFraudTransactionPercentage(5);
    gen.setEnrichCustomers(true);
    gen.setEnrichPaymentCard(true);
    gen.setEnrichProduct(true);
    gen.setEnrichStorePOS(true);
    gen.setNoCards(15000);
    gen.setNoCustomers(10000);
    gen.setNoPOS(5000);
    gen.setNoProducts(20000);
    return gen;
  }

}
