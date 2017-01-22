package Bank;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import java.sql.Connection;
import java.sql.Statement;


/**
 * Created by tiago on 1/21/17.
 */
public class Bank {

    static void receiveTransaction(){
        try {
            Context ctx = new InitialContext();
            UserTransaction txn = (UserTransaction) ctx.lookup("java:comp/UserTransaction");
            ConnectionFactory cf = (ConnectionFactory) ctx.lookup("jms/settlementConnection");
            javax.jms.Connection c1 = cf.createConnection();
            c1.start();
            Session s = c1.createSession(false, 0);
            Queue q = s.createQueue("FILA1");
            MessageConsumer mc = s.createConsumer(q);
            TextMessage m = (TextMessage) mc.receive();
            if (m != null) System.out.println(m.getText());
            m.clearBody();
            mc.close();
            s.close();
            c1.close();
            txn.begin();
            DataSource ds2 = (DataSource) ctx.lookup("jdbc/banco");
            Connection c2 = ds2.getConnection();
            Statement s2 = c2.createStatement();
            s2.executeUpdate("update conta set saldo = saldo - 1 where user_name='tiago'");
            s2.close();
            c2.close();
            txn.commit();


        }
        catch(Exception e) {}
    }

    public static void main(String[] args){
        while (true) {
           receiveTransaction();
        }
    }
}
