package bolsa;

import co.paralleluniverse.actors.Actor;
import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.io.FiberServerSocketChannel;
import co.paralleluniverse.fibers.io.FiberSocketChannel;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by lima on 12/18/16.
 */
public class Exchange {

    //Basic classes for message creation
    private static Map <String, String> listofusers= new HashMap<String,String>();
    private static ArrayList<String> empresas = new ArrayList<String>();
    private enum Type { DATA, EOF, IOE, ENTER, LEAVE, LINE, AUTH,INFO, SELLORDER,BUYORDER}
    private static int MAXLEN=1024;
    static class Msg {
        final Type type;
        final Object o;  // careful with mutable objects, such as the byte array
        Msg(Type type, Object o) { this.type = type; this.o = o; }
    }

    //Auxiliary comunication class, it digests Strings

    static class Authentication extends BasicActor <Msg,Void> {
        final ActorRef<Msg> dest;
        final FiberSocketChannel socket;

        Authentication(ActorRef<Msg> dest, FiberSocketChannel socket) {
            this.dest = dest;
            this.socket = socket;
        }

        protected Void doRun() throws InterruptedException, SuspendExecution {
            ByteBuffer in = ByteBuffer.allocate(MAXLEN);
            CodedInputStream cis;
            try {
                if (socket.read(in) <= 0) this.dest.send(new Msg(Type.EOF,null));
                in.flip();
                cis = CodedInputStream.newInstance(in);
                int userSize = cis.readInt32();
                in.clear();
                in.compact();
                byte [] userbytes = cis.readRawBytes(userSize);
                Utilizador.User user = Utilizador.User.parseFrom(userbytes);
                if(listofusers.containsKey(user.getUsername())) {
                    if(listofusers.get(user.getUsername()).equals(user.getPassword())){
                        System.out.println("Conetado");
                        this.dest.send(new Msg(Type.AUTH,null));
                    }
                    else{
                        System.out.println("Disconetado");
                        this.dest.send(new Msg(Type.IOE,null));
                    }
                }
                else{
                    System.out.println("Disconetado");
                    this.dest.send(new Msg(Type.IOE,null));}
            }
            catch(Exception e){e.printStackTrace();}
            return null;
        }
    }

    static class ObjReader extends BasicActor <Msg,Void> {
        final ActorRef<Msg> dest;
        final ActorRef<Msg> exchange;
        final FiberSocketChannel socket;

        ObjReader(ActorRef<Msg> dest, ActorRef<Msg> exchange, FiberSocketChannel socket) {
            this.dest = dest;
            this.socket = socket;
            this.exchange = exchange;
        }

        protected Void doRun() throws InterruptedException, SuspendExecution {
            ByteBuffer in = ByteBuffer.allocate(MAXLEN);

            try {
                for (;;) {
                    CodedInputStream cis;
                    byte b;
                    if (socket.read(in) <= 0) this.dest.send(new Msg(Type.EOF, null));
                    in.flip();
                    cis = CodedInputStream.newInstance(in);
                    int flag = cis.readInt32();
                    int objSize = cis.readInt32();
                    byte[] object = cis.readRawBytes(objSize);
                    in.clear();
                    if (flag == 1000){
                        Ordem.Order ordemVenda =  Ordem.Order.parseFrom(object);
                        this.exchange.send(new Msg(Type.SELLORDER,ordemVenda));
                    }
                    else if (flag == 1001){
                        Ordem.Order ordemCompra = Ordem.Order.parseFrom(object);
                        this.exchange.send(new Msg(Type.BUYORDER,ordemCompra));
                    }
                     else {
                        System.out.println("Falhou");
                        this.dest.send(new Msg(Type.IOE, null));
                    }
                }
            }
            catch(Exception e){e.printStackTrace();}
            return null;
        }
    }

    static class CompanyOrders extends BasicActor <Msg,Void> {//Esta classe trata de gerar os publishing servers aos quais se irão ligar os subscritores, adicionalmente irá tratar de todas as exchanges ligadas a uma determinada empresa
        private String empresa;
        private ActorRef<Msg> exchange;
        private TreeMap<Double,Ordem.Order> sellOrder = null;
        private TreeMap<Double,Ordem.Order> buyOrder = null;
        private ZMQ.Context context = ZMQ.context(1);
        private ZMQ.Socket publisher = context.socket(ZMQ.PUB);

        CompanyOrders(String empresa,ActorRef<Msg>exchange){
            this.exchange=exchange;
            this.empresa=empresa;
            sellOrder = new TreeMap<Double, Ordem.Order>();
            buyOrder = new TreeMap<Double, Ordem.Order>();

        }

        protected Void doRun() throws InterruptedException, SuspendExecution{
            publisher.bind("tcp://127.0.0.1:4");
            while (receive(msg -> {
                switch (msg.type) {
                    case SELLORDER:
                        Ordem.Order ordemVenda = (Ordem.Order) msg.o;
                        sellOrder.put(ordemVenda.getPreco(),ordemVenda);
                        if (buyOrder.isEmpty()) return true;
                        else {
                            Ordem.Order orderSell = sellOrder.firstEntry().getValue();
                            Ordem.Order orderBuy = buyOrder.firstEntry().getValue();
                            // CRIAR TRANSAÇAO CUIDADO COM A DIFERRENÇA DE UNIDADES
                            //sellOrder.remove(orderSell.getEmpresa());
                            //buyOrder.remove(orderBuy.getEmpresa());
                            publisher.sendMore(this.empresa);
                            publisher.send("aqui caralho");

                        }
                        return true;
                    case BUYORDER:
                        Ordem.Order ordemCompra = (Ordem.Order) msg.o;
                        buyOrder.put(ordemCompra.getPreco(),ordemCompra);
                        if (sellOrder.isEmpty()) return true;
                        else{
                            Ordem.Order orderSell = sellOrder.firstEntry().getValue();
                            Ordem.Order orderBuy = buyOrder.firstEntry().getValue();
                            // CRIAR TRANSAÇAO CUIDADO COM A DIFERENÇA DE UNIDADES
                            //sellOrder.remove(orderSell.getEmpresa());
                            //buyOrder.remove(orderBuy.getEmpresa());
                        }
                        return true;
                }
                return false;
            }));
            return null;
        }

    }

    static class ExchangeInstance extends BasicActor<Msg, Void> {
        private Set<ActorRef> users = new HashSet<>();
        private Map<String,ActorRef> empresasToActors= new HashMap<>();

        ExchangeInstance(Map<String,ActorRef> empresasToActors){
            this.empresasToActors = empresasToActors;
        }

        protected Void doRun() throws InterruptedException, SuspendExecution {
            while (receive(msg -> {
                switch (msg.type) {
                    case ENTER:
                        users.add((ActorRef) msg.o);
                        return true;
                    case LEAVE:
                        users.remove(msg.o);
                        return true;
                    case LINE:
                        for (ActorRef u : users) u.send(msg);
                        return true;
                    case SELLORDER:
                        Ordem.Order ordemVenda = (Ordem.Order) msg.o;
                        if (!empresas.contains(ordemVenda.getEmpresa())){
                            System.out.println("A empresa não Existe");
                             return true;}
                        else{
                            empresasToActors.get(ordemVenda.getEmpresa()).send(new Msg(Type.SELLORDER,ordemVenda));
                        }
                        return  true;
                    case BUYORDER:
                        Ordem.Order ordemCompra = (Ordem.Order) msg.o;
                        if (!empresas.contains(ordemCompra.getEmpresa())){
                            System.out.println("A empresa não Existe");
                            return true;}
                        else{
                            empresasToActors.get(ordemCompra.getEmpresa()).send(new Msg(Type.BUYORDER,ordemCompra));
                        }
                        return  true;
                }
                return false;
            }));
            return null;
        }
    }

    static class Acceptor extends BasicActor {
        final int port;
        final ActorRef exchange;
        Acceptor(int port, ActorRef exchange) { this.port = port; this.exchange = exchange; }

        protected Void doRun() throws InterruptedException, SuspendExecution {
            try {
                FiberServerSocketChannel ss = FiberServerSocketChannel.open();
                ss.bind(new InetSocketAddress(port));
                while (true) {
                    FiberSocketChannel socket = ss.accept();
                    new Client(this.exchange, socket).spawn();

                }
            } catch (IOException e) { }
            return null;
        }
    }

    static class Client extends BasicActor<Msg, Void> {
        final ActorRef exchange;
        final FiberSocketChannel socket;
        private static boolean loggedIn = false;
        Client(ActorRef exchange, FiberSocketChannel socket) { this.exchange = exchange; this.socket = socket; }

        protected Void doRun() throws InterruptedException, SuspendExecution {
           // new LineReader(self(), socket).spawn(); //para já vamos ignorar a utilização de um lineReader, é importante remover o flush
            new Authentication(self(),socket).spawn();
            exchange.send(new Msg(Type.ENTER, self()));
            while (receive(msg -> {
                try {
                    switch (msg.type) {
                        case AUTH:
                            loggedIn = true;
                            new ObjReader(self(),exchange,socket).spawn();
                            return true;
                        case DATA:
                            exchange.send(new Msg(Type.INFO, msg.o));
                            return true;
                        case EOF:
                            exchange.send(new Msg(Type.LEAVE,self()));
                            socket.close();
                            return false;
                        case IOE:
                            exchange.send(new Msg(Type.LEAVE, self()));
                            socket.close();
                            return false;
                        case LINE:
                            socket.write(ByteBuffer.wrap((byte[])msg.o));
                            return true;
                    }
                } catch (IOException e) {
                    exchange.send(new Msg(Type.LEAVE, self()));
                }
                return false;  // stops the actor if some unexpected message is received
            }));
            return null;
        }
    }

    private static void generateUsersAndCompanies(){
        listofusers.put("tiago","tiago");
        listofusers.put("rafuru","rafuru");
        listofusers.put("desu","desu");
        empresas.add("Empresa1");
        empresas.add("Empresa2");
        empresas.add("Empresa3");
        empresas.add("Empresa4");
    }

    public static void main(String[] args) throws Exception {
        generateUsersAndCompanies();
        int port = 7777; //Integer.parseInt(args[0]);
        Map<String,ActorRef> empresasToActors = new HashMap<String,ActorRef>();
        ActorRef exchange = new ExchangeInstance(empresasToActors).spawn();
        for (int i=0; i<empresas.size();i++){
            empresasToActors.put(empresas.get(i),new CompanyOrders(empresas.get(i),exchange).spawn());
        }
        Acceptor acceptor = new Acceptor(port, exchange);
        acceptor.spawn();
        acceptor.join();
    }
}
