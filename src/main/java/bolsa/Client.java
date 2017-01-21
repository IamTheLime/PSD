package bolsa;

import com.google.protobuf.CodedOutputStream;
import org.zeromq.ZMQ;

import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by lima on 12/18/16.
 */

class Receive_Data extends Thread{
    private Socket connection;

    public Receive_Data(Socket connection){
        this.connection=connection;
    }
    public void run(){
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String buffer;
            while((buffer = reader.readLine())!=null){ System.out.println(buffer);}
        }
        catch(Exception e){
            System.out.println("Disconetado do servidor");
        }
    }
    
}

class Show_Subscriber extends Thread{

    public void run(){
        String empresa = "Empresa1";
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
        subscriber.connect("tcp://localhost:6666");
        subscriber.subscribe(empresa.getBytes());
        // Read envelope with address
        String address = subscriber.recvStr ();
        // Read message contents
        String contents = subscriber.recvStr ();
        System.out.println(address + " : " + contents);
    }
}

class Send_Data extends Thread {
    private Socket connection;
    ArrayList<String> empresas = new ArrayList<String>();

    public Send_Data(Socket connection) {
        this.connection = connection;
    }

    public void run() {
        //Realização do log in
        try {
            Scanner in = new Scanner(System.in);
            System.out.println("Por favor introduza o seu utilizador:");
            String username = in.next();
            System.out.println("Por favor introduza a sua password:");
            String password = in.next();
            CodedOutputStream cos = CodedOutputStream.newInstance(connection.getOutputStream());
            Utilizador.User usercreation = createUser(username, password);
            byte[] user_byte = usercreation.toByteArray();
            cos.writeInt32NoTag(user_byte.length);
            cos.writeRawBytes(user_byte);
            cos.flush();
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket subscriber = context.socket(ZMQ.SUB);

            boolean exit = false;
            int menu = 0;
            if (!(connection.isConnected() && !connection.isClosed())) menu = 5;
            for (; ; ) {
                switch (menu) {
                    case 1://Ordens de Venda
                        System.out.println("Introduza o nome da empresa");
                        String empresavenda = in.next();
                        System.out.println("Introduza a quantidade de ações a vender");
                        int quantidadevenda = Integer.parseInt(in.next());
                        System.out.println("Introduza o preço mínimo");
                        double precominimo = Float.parseFloat(in.next());
                        Ordem.Order ordemvenda = createOrder(Ordem.Order.OrderTypes.SELL,empresavenda,username,precominimo,quantidadevenda);
                        byte[] sell_byte = ordemvenda.toByteArray();
                        cos.writeInt32NoTag(1000);
                        cos.writeInt32NoTag(sell_byte.length);
                        cos.writeRawBytes(sell_byte);
                        cos.flush();

                        menu = 0;
                        break;
                    case 2://Ordem de Compra
                        System.out.println("Introduza o nome da empresa");
                        String empresacompra = in.next();
                        System.out.println("Introduza a quantidade de ações a comprar");
                        int quantidadecompra = Integer.parseInt(in.next());
                        System.out.println("Introduza o preço máximo");
                        double precomaximo = Float.parseFloat(in.next());
                        Ordem.Order ordemcompra = createOrder(Ordem.Order.OrderTypes.BUY,empresacompra,username,precomaximo,quantidadecompra);
                        byte[] buy_byte = ordemcompra.toByteArray();
                        cos.writeInt32NoTag(1001);
                        cos.writeInt32NoTag(buy_byte.length);
                        cos.writeRawBytes(buy_byte);
                        cos.flush();
                        menu = 0;
                        break;
                    case 3:
                        System.out.println("Introduza o nome da empresa");
                        String empresa = in.next();
                        empresas.add(empresa);
                        menu = 0;
                        break;
                    case 4:
                        Show_Subscriber ss = new Show_Subscriber();
                        ss.run();

                        menu = 4;
                        break;
                    case 5:
                        exit = true;
                        break;
                    default:
                        System.out.println("Por favor selecione uma opção:");
                        System.out.println("| 1 | Ordens de Venda");
                        System.out.println("| 2 | Ordens de Compra");
                        System.out.println("| 3 | Seguir Empresa");
                        System.out.println("| 4 | Ver Subscrições");
                        System.out.println("| 5 | Sair do Programa");
                        System.out.println("Por favor selecione uma opção:");
                        menu = Integer.parseInt(in.next());
                        break;
                }
                if (exit) {
                    connection.close();
                    break;
                }
            }
        } catch (Exception e) {
            try{
            this.connection.close();} catch(Exception e2) {e2.printStackTrace();}
            System.out.println("Disconetado do servidor");
        }

        /*
        BufferedReader inputFromConsole = new BufferedReader(new InputStreamReader(System.in));
        try {
            PrintWriter writer = new PrintWriter(this.connection.getOutputStream());
            String buffer;
            while ((buffer = inputFromConsole.readLine()) != null) {
                writer.println(buffer);
                writer.flush();
            }
        }
        catch (Exception e) {e.printStackTrace();}*/
    }

    static Utilizador.User createUser(String username, String password) {
        return
                Utilizador.User.newBuilder()
                        .setUsername(username)
                        .setPassword(password)
                        .build();

    }

    static Ordem.Order createOrder(Ordem.Order.OrderTypes typeoforder, String companyName, String userName, double preco, int quantidade){
        return
                Ordem.Order.newBuilder()
                        .setEmpresa(companyName)
                        .setPreco(preco)
                        .setUsername(userName)
                        .setTipodeempresa(typeoforder)
                        .setQuantidade(quantidade)
                        .build();
    }
}

public class Client {
    public static void main(String []argv){
        try{
            Socket connection_to_exchange= new Socket("localhost",7777);
            new Send_Data(connection_to_exchange).start();
            new Receive_Data(connection_to_exchange).start();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}