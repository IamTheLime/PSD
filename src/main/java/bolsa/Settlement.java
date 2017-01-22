package bolsa;


import com.google.protobuf.CodedInputStream;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;


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

            }


        }

        catch (Exception e){e.printStackTrace();}
        }
}
