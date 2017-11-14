
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

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
import com.datatorrent.cep.transactionGenerator.TransactionGenerator;

/**
 * Created by ambarish on 25/10/17.
 */
public class LatencyTests
{

  public static void main(String args[]){

    int numTransactions = 30000;
    ArrayList<Long> times = new ArrayList<Long>();
    TransactionGenerator gen = new  TransactionGenerator();
    gen.setFraudTransactionPercentage(30);
    gen.setEnrichCustomers(true);
    gen.setEnrichPaymentCard(true);
    gen.setEnrichProduct(true);
    gen.setEnrichStorePOS(true);

    try {
      gen.generateData();
    } catch (IOException e) {
      e.printStackTrace();
    }

    KieServices kieServices = KieServices.Factory.get();
    KieContainer kieContainer = kieServices.newKieClasspathContainer();
    KieSession kieSession = kieContainer.newKieSession();
    kieSession.addEventListener(new RulesFiredListener());
    kieSession.addEventListener(new RuleEventListener());
    long start1;
    long start = System.currentTimeMillis();
    for (int i = 0; i < numTransactions; i++){
        start1 = System.nanoTime();
        kieSession.insert(gen.generateTransaction(null));
        kieSession.fireAllRules();
        times.add((System.nanoTime() - start1));
    }
    long time = System.currentTimeMillis() - start;

    System.out.println("Processed : " + numTransactions + " Time : " + time);
    try {
      PrintWriter writer = new PrintWriter("times-"+numTransactions+"-"+time+".txt", "UTF-8");
      for (Long i : times){
        writer.println(i);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }


    kieSession.dispose();
    kieSession.destroy();
  }

  private static class RuleEventListener implements RuleRuntimeEventListener
  {

    int count  = 0;
    @Override
    public void objectInserted(ObjectInsertedEvent event)
    {
      count++;
        if(count%100==0){
          System.out.print("\n");
        }
      System.out.print(".");
    }

    @Override
    public void objectUpdated(ObjectUpdatedEvent event)
    {

    }

    @Override
    public void objectDeleted(ObjectDeletedEvent event)
    {
//      System.out.println("Object Deleted");
    }
  }

  private static class RulesFiredListener implements AgendaEventListener{

    @Override
    public void matchCreated(MatchCreatedEvent matchCreatedEvent)
    {

    }

    @Override
    public void matchCancelled(MatchCancelledEvent matchCancelledEvent)
    {

    }

    @Override
    public void beforeMatchFired(BeforeMatchFiredEvent event)
    {

    }

    @Override
    public void afterMatchFired(AfterMatchFiredEvent event)
    {
//      Rule matchedRule = event.getMatch().getRule();
//      if(ruleCounts.containsKey(matchedRule.getName())){
//          ruleCounts.put(matchedRule.getName(),ruleCounts.get(matchedRule.getName())+1);
//      }else{
//        ruleCounts.put(matchedRule.getName(),1);
//      }
//      System.out.println(matchedRule.getName());
    }

    @Override
    public void agendaGroupPopped(AgendaGroupPoppedEvent agendaGroupPoppedEvent)
    {

    }

    @Override
    public void agendaGroupPushed(AgendaGroupPushedEvent agendaGroupPushedEvent)
    {

    }

    @Override
    public void beforeRuleFlowGroupActivated(RuleFlowGroupActivatedEvent ruleFlowGroupActivatedEvent)
    {

    }

    @Override
    public void afterRuleFlowGroupActivated(RuleFlowGroupActivatedEvent ruleFlowGroupActivatedEvent)
    {

    }

    @Override
    public void beforeRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent ruleFlowGroupDeactivatedEvent)
    {

    }

    @Override
    public void afterRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent ruleFlowGroupDeactivatedEvent)
    {

    }
  }
}
