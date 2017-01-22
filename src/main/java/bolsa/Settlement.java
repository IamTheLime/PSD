package bolsa;

import jdk.nashorn.internal.ir.CatchNode;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import java.lang.reflect.Executable;
import java.sql.Connection;
import java.sql.Statement;
/**
 * Created by tiago on 1/21/17.
 */
public class Settlement {

    public static void main(String[] args) {
        try {
            Context ctx = new InitialContext();
            UserTransaction txn = (UserTransaction) ctx.lookup("java:comp/UserTransaction");

            txn.begin();
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup("jms/settlementConnection");
            javax.jms.Connection c1 = cf.createConnection();
            c1.start();
            Session s = c1.createSession(false, 0);
            Topic q = s.createTopic("TransactionToSettlement");
            MessageConsumer mc = s.createDurableSubscriber(q, "teste");
            ObjectMessage obj = (ObjectMessage) mc.receive();
            System.out.println(((Transacao.Transaction) obj.getObject()).getEmpresa());
            txn.commit();
        }
        catch (Exception e){e.printStackTrace();}
    }
}
