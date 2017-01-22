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
            subscriber.connect("tcp://localhost:6667");
            subscriber.subscribe("Transaction".getBytes());

            while(true){
                String aux = subscriber.recvStr(Charset.defaultCharset());
                System.out.println(aux);

                Context ctx = new InitialContext();

                UserTransaction txn = (UserTransaction) ctx.lookup("java:comp/UserTransaction");

                txn.begin();

                ConnectionFactory cf = (ConnectionFactory) ctx.lookup("jms/settlementConnection");
                javax.jms.Connection c2 = cf.createConnection();
                Session s = c2.createSession(false, 0);
                Queue q = s.createQueue("FILA1");
                MessageProducer p = s.createProducer(q);

                TextMessage m = s.createTextMessage(aux);
                m.setIntProperty("price", 10);
                p.send(m);

                p.close();
                s.close();
                c2.close();

                txn.commit();


            }


        }

        catch (Exception e){e.printStackTrace();}
        }
}
