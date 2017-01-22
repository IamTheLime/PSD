package bolsa;


import com.google.protobuf.CodedInputStream;
import org.zeromq.ZMQ;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Statement;


/**
 * Created by tiago on 1/21/17.
 */
public class Settlement {

    static private int MAXLEN = 1024;

    public static void main(String[] args) {

        try {

            ZMQ.Context context = ZMQ.context(2);
            ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
            ZMQ.Context context2 = ZMQ.context(3);
            ZMQ.Socket publisher = context2.socket(ZMQ.PUB);
            subscriber.connect("tcp://localhost:6667");
            subscriber.subscribe("Transaction".getBytes());
            publisher.connect("tcp://localhost:6668");
            String aux = null;

            while((aux = subscriber.recvStr(Charset.defaultCharset()))!=null){

                if (aux.equals("Transaction")) continue;
                Context ctx = new InitialContext();
                UserTransaction txn = (UserTransaction) ctx.lookup("java:comp/UserTransaction");

                txn.begin();
                ConnectionFactory cf = (ConnectionFactory) ctx.lookup("jms/settlementConnection");
                javax.jms.Connection c1 = cf.createConnection();
                Session s = c1.createSession(false, 0);
                Queue q = s.createQueue("FILA1");
                MessageProducer p = s.createProducer(q);
                TextMessage m = s.createTextMessage(aux);
                p.send(m);
                p.close();
                s.close();
                c1.close();

                String[] parts = aux.split("\n");

                DataSource ds2 = (DataSource) ctx.lookup("jdbc/settlement");
                Connection c2 = ds2.getConnection();
                Statement s2 = c2.createStatement();
                Statement s3 = c2.createStatement();
                String aux2 = "update acoes set quantidade = quantidade - "+ parts[4] +" where user_name='"+parts[2]+"' and empresa = '"+parts[0]+"'";
                s2.executeUpdate(aux2);
                s3.executeUpdate("update acoes set quantidade = quantidade + "+ parts[4] +" where user_name='"+parts[1]+"' and empresa = '"+parts[0]+"'");
                s2.close();
                s3.close();
                c2.close();

                txn.commit();

                publisher.sendMore("TransactionComplete");
                publisher.send(aux);

            }


        }

        catch (Exception e){e.printStackTrace();}
        }
}
