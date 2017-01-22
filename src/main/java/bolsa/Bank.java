package bolsa;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.transaction.UserTransaction;

/**
 * Created by tiago on 1/21/17.
 */
public class Bank {


    public static void main(String[] args) throws Exception{
        Context ctx = new InitialContext();
        UserTransaction txn = (UserTransaction) ctx.lookup("java:comp/UserTransaction");

        txn.begin();
        ConnectionFactory cf = (ConnectionFactory) ctx.lookup("jms/bankConnection");
        javax.jms.Connection c1 = cf.createConnection();
        c1.start();
        Session s = c1.createSession(false,0);
        Topic q = s.createTopic("TransactionToSettlement");
        MessageConsumer mc = s.createDurableSubscriber(q,"teste");
        ObjectMessage obj = (ObjectMessage) mc.receive();
        System.out.println(((Transacao.Transaction)obj.getObject()).getEmpresa());
        txn.commit();

    }
}
