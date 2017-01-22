package bolsa;

import co.paralleluniverse.actors.Actor;
import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.actors.MessageProcessor;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.io.FiberServerSocketChannel;
import co.paralleluniverse.fibers.io.FiberSocketChannel;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.zeromq.ZMQ;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.transaction.UserTransaction;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by lima on 12/18/16.
 */
public class Exchange {

    //Basic classes for message creation
    private static Map <String, String> listofusers= new HashMap<String,String>();
    private static ArrayList<String> empresas = new ArrayList<String>();
    private enum Type { EOF, IOE, ENTER, LEAVE, LINE, AUTH,INFO,BUYER,SELLER,SELLORDER,BUYORDER}
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
                        this.dest.send(new Msg(Type.AUTH,user.getUsername()));
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

    static class TransactionManager extends BasicActor<Msg,Void> {
        private int transaction_quantity;
        private String buyer;
        private String seller;
        private double agreedprice;
        private String empresa;
        private ZMQ.Socket publisher;

        TransactionManager(int quantity, String buyer, String seller, double agreedprice, String empresa, ZMQ.Socket publisher){
            this.transaction_quantity = quantity;
            this.buyer = buyer;
            this.seller = seller;
            this.agreedprice = agreedprice;
            this.empresa = empresa;
            this.publisher=publisher;
        }

        static Transacao.Transaction createTransaction(String empresa, String buyer, String seller, int quantidade, double agreedprice){
            return
                    Transacao.Transaction.newBuilder()
                            .setEmpresa(empresa)
                            .setBuyer(buyer)
                            .setSeller(seller)
                            .setQuantidade(quantidade)
                            .setPreco(agreedprice)
                            .build();
        }


        protected Void doRun() throws InterruptedException, SuspendExecution {


            publisher.sendMore("Transaction");
                publisher.send(this.empresa+"\n"+
                               this.buyer+"\n"+
                               this.seller+"\n"+
                               this.agreedprice+"\n"+
                               this.transaction_quantity);

            return null;

        }

    }

    static class CompanyOrders extends BasicActor <Msg,Void> {//Esta classe trata de gerar os publishing servers aos quais se irão ligar os subscritores, adicionalmente irá tratar de todas as exchanges ligadas a uma determinada empresa
        private String empresa;
        private ActorRef<Msg> exchange;
        private TreeMap<Double,Stack<Ordem.Order>> sellOrder = null;
        private TreeMap<Double,Stack<Ordem.Order>> buyOrder = null;
        private ZMQ.Socket publisher = null;
        private ZMQ.Socket publisher2 = null;

        CompanyOrders(String empresa,ActorRef<Msg>exchange,ZMQ.Socket publisher,ZMQ.Socket publisher2){
            this.exchange=exchange;
            this.empresa=empresa;
            this.publisher=publisher;
            this.publisher2=publisher2;
            sellOrder = new TreeMap<Double, Stack<Ordem.Order>>();
            buyOrder = new TreeMap<Double, Stack<Ordem.Order>>();

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

        protected Void doRun() throws InterruptedException, SuspendExecution{
            while (receive(msg -> {
                switch (msg.type) {
                    case SELLORDER:
                        Ordem.Order ordemVenda = (Ordem.Order) msg.o;
                        publisher.sendMore(this.empresa);
                        publisher.send("***Ordem de Venda***\n" +
                                "Preço máximo: "+ordemVenda.getPreco()+"\n"+
                                "Quantidade: "+ordemVenda.getQuantidade());
                        Stack <Ordem.Order> aux =sellOrder.get(ordemVenda.getPreco());
                        if ((aux) !=null){
                            aux.push(ordemVenda);
                        }
                        else{
                            Stack<Ordem.Order> stack = new Stack<Ordem.Order>();
                            stack.push(ordemVenda);
                            sellOrder.put(ordemVenda.getPreco(),stack);
                        }
                        if (buyOrder.isEmpty()) return true;
                        else {
                            Stack<Ordem.Order> orderSellStack = sellOrder.firstEntry().getValue();
                            Ordem.Order orderSell = orderSellStack.pop();
                            if (orderSellStack.size() == 0) {sellOrder.remove(sellOrder.firstEntry().getKey());}
                            Stack<Ordem.Order> orderBuyStack = buyOrder.firstEntry().getValue();
                            Ordem.Order orderBuy = orderBuyStack.pop();
                            if (orderBuyStack.size() == 0) {buyOrder.remove(buyOrder.firstEntry().getKey());}
                            if(orderSell.getQuantidade() == orderBuy.getQuantidade()){
                                new TransactionManager(
                                        orderBuy.getQuantidade(),
                                        orderBuy.getUsername(),
                                        orderSell.getUsername(),
                                        (orderSell.getPreco()+orderBuy.getPreco())/2,
                                        empresa,
                                        publisher2)
                                            .spawn();
                            }
                            else if(orderSell.getQuantidade() < orderBuy.getQuantidade()){
                                self().send(new Msg(Type.BUYORDER,createOrder(Ordem.Order.OrderTypes.BUY,empresa,orderBuy.getUsername(),orderBuy.getPreco(),orderBuy.getQuantidade()-orderSell.getQuantidade())));
                                new TransactionManager(
                                        orderSell.getQuantidade(),
                                        orderBuy.getUsername(),
                                        orderSell.getUsername(),
                                        (orderSell.getPreco()+orderBuy.getPreco())/2,
                                        empresa,
                                        publisher2)
                                        .spawn();
                            }
                            else {
                                self().send(new Msg(Type.SELLORDER,createOrder(Ordem.Order.OrderTypes.SELL,empresa,orderSell.getUsername(),orderSell.getPreco(),orderSell.getQuantidade()-orderBuy.getQuantidade())));
                                new TransactionManager(
                                        orderBuy.getQuantidade(),
                                        orderBuy.getUsername(),
                                        orderSell.getUsername(),
                                        (orderSell.getPreco()+orderBuy.getPreco())/2,
                                        empresa,
                                        publisher2)
                                        .spawn();
                            }

                        }
                        return true;
                    case BUYORDER:
                        Ordem.Order ordemCompra = (Ordem.Order) msg.o;
                        publisher.sendMore(this.empresa);
                        publisher.send("***Ordem de Compra***\n" +
                                       "Empresa: "+ordemCompra.getEmpresa()+"\n" +
                                        "Preço máximo: "+ordemCompra.getPreco()+"\n"+
                                        "Quantidade: "+ordemCompra.getQuantidade());

                        Stack <Ordem.Order> aux2 =buyOrder.get(ordemCompra.getPreco());
                        if ((aux2) !=null){
                            aux2.push(ordemCompra);
                        }
                        else{
                            Stack<Ordem.Order> stack = new Stack<Ordem.Order>();
                            stack.push(ordemCompra);
                            buyOrder.put(ordemCompra.getPreco(),stack);
                        }
                        if (sellOrder.isEmpty()) return true;
                        else {
                            Stack<Ordem.Order> orderSellStack = sellOrder.firstEntry().getValue();
                            Ordem.Order orderSell = orderSellStack.pop();
                            if (orderSellStack.size() == 0) {sellOrder.remove(sellOrder.firstEntry().getKey());}
                            Stack<Ordem.Order> orderBuyStack = buyOrder.firstEntry().getValue();
                            Ordem.Order orderBuy = orderBuyStack.pop();
                            if (orderBuyStack.size() == 0) {buyOrder.remove(buyOrder.firstEntry().getKey());}
                            if(orderSell.getQuantidade() == orderBuy.getQuantidade()){
                                new TransactionManager(
                                        orderBuy.getQuantidade(),
                                        orderBuy.getUsername(),
                                        orderSell.getUsername(),
                                        (orderSell.getPreco()+orderBuy.getPreco())/2,
                                        empresa,
                                        publisher2)
                                        .spawn();
                            }
                            else if(orderSell.getQuantidade() < orderBuy.getQuantidade()){
                                self().send(new Msg(Type.BUYORDER,createOrder(Ordem.Order.OrderTypes.BUY,empresa,orderBuy.getUsername(),orderBuy.getPreco(),orderBuy.getQuantidade()-orderSell.getQuantidade())));
                                new TransactionManager(
                                        orderSell.getQuantidade(),
                                        orderBuy.getUsername(),
                                        orderSell.getUsername(),
                                        (orderSell.getPreco()+orderBuy.getPreco())/2,
                                        empresa,
                                        publisher2)
                                        .spawn();
                            }
                            else {
                               self().send(new Msg(Type.SELLORDER,createOrder(Ordem.Order.OrderTypes.SELL,empresa,orderSell.getUsername(),orderSell.getPreco(),orderSell.getQuantidade()-orderBuy.getQuantidade())));
                                new TransactionManager(
                                        orderBuy.getQuantidade(),
                                        orderBuy.getUsername(),
                                        orderSell.getUsername(),
                                        (orderSell.getPreco()+orderBuy.getPreco())/2,
                                        empresa,
                                        publisher2)
                                        .spawn();
                            }
                        }
                        return true;
                }
                return false;
            }));
            return null;
        }

    }
  static class CheckTransactions extends Thread{
        private ZMQ.Socket subscriber = null;
        private ActorRef exchange = null;

        CheckTransactions(ZMQ.Socket subscriber, ActorRef exchange){
            this.exchange = exchange;
            this.subscriber = subscriber;
        }

        public void run()
        {
            subscriber.subscribe("TransactionComplete".getBytes());
            String aux = null;
            while((aux = subscriber.recvStr(Charset.defaultCharset()))!=null){
                if(aux.equals("TransactionComplete")) continue;
                try{
                exchange.send(new Msg(Type.LINE,aux));}
                catch(Exception e){};
            }
        }
    }

    static class ExchangeInstance extends BasicActor<Msg, Void> {
        private Map<String,ActorRef> users = new HashMap<>();
        private Map<String,ActorRef> empresasToActors= new HashMap<>();

        ExchangeInstance(Map<String,ActorRef> empresasToActors){
            this.empresasToActors = empresasToActors;
        }

        protected Void doRun() throws InterruptedException, SuspendExecution {
            while (receive(msg -> {
                switch (msg.type) {
                    case ENTER:
                        users.put(((Pair) msg.o).username,((Pair) msg.o).actor);
                        return true;
                    case LEAVE:
                        users.remove(msg.o);
                        return true;
                    case LINE:
                        String [] parts = ((String)msg.o).split("\n");
                        for (String part : parts) System.out.println("LALALA"+part);
                        users.get(parts[1]).send(new Msg(Type.BUYER,parts));
                        users.get(parts[2]).send(new Msg(Type.SELLER,parts));
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

    static class Pair {
        public ActorRef<Msg> actor;
        public String username;

        public Pair (ActorRef<Msg> actor,String username) {
            this.actor = actor;
            this.username = username;
        }
    }

    static class Client extends BasicActor<Msg, Void> {
        final ActorRef exchange;
        final FiberSocketChannel socket;
        private static boolean loggedIn = false;
        Client(ActorRef exchange, FiberSocketChannel socket) { this.exchange = exchange; this.socket = socket; }

        protected Void doRun() throws InterruptedException, SuspendExecution {
            new Authentication(self(),socket).spawn();
            while (receive(msg -> {
                try {
                    switch (msg.type) {
                        case AUTH:
                            loggedIn = true;
                            new ObjReader(self(),exchange,socket).spawn();
                            exchange.send(new Msg(Type.ENTER, new Pair(self(),(String) msg.o))); //AQUI
                            return true;
                        case BUYER:
                            ByteBuffer bbfer = ByteBuffer.allocate(MAXLEN);
                            CodedOutputStream cos = CodedOutputStream.newInstance(bbfer);
                            String [] parts = (String [])msg.o;
                            String aux = "*****Notificação******\n***Realizou a compra de "+ parts[4] +" acções a custo "+parts[3]+"****\n********";
                            //cos.writeInt32NoTag(aux.length());
                            cos.writeStringNoTag(aux);
                            cos.flush();
                            bbfer.flip();
                            socket.write(bbfer);
                            return true;
                        case SELLER:
                            ByteBuffer bbfer2 = ByteBuffer.allocate(MAXLEN);
                            CodedOutputStream cos2 = CodedOutputStream.newInstance(bbfer2);
                            String [] parts2 = (String [])msg.o;
                            String aux2 = "*****Notificação******\n***Realizou a venda de "+ parts2[4] +" acções a custo "+parts2[3]+"***\n********";
                            //cos2.writeInt32NoTag(aux2.length());
                            cos2.writeStringNoTag(aux2);
                            cos2.flush();
                            bbfer2.flip();
                            socket.write(bbfer2);
                            return true;
                        case EOF:
                            exchange.send(new Msg(Type.LEAVE,self()));
                            socket.close();
                            return false;
                        case IOE:
                            exchange.send(new Msg(Type.LEAVE, self()));
                            socket.close();
                            return false;
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
        listofusers.put("rafael","rafael");
        listofusers.put("lima","lima");
        listofusers.put("alexandre","alexandre");
        listofusers.put("daniela","daniela");
        listofusers.put("bruno","bruno");
        listofusers.put("azevedo","azevedo");

        empresas.add("Empresa1");
        empresas.add("Empresa2");
        empresas.add("Empresa3");
        empresas.add("Empresa4");
        empresas.add("Empresa5");
    }

    public static void main(String[] args) throws Exception {
        generateUsersAndCompanies();
        int port = 7777; //Integer.parseInt(args[0]);
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket publisher = context.socket(ZMQ.PUB);
        publisher.bind("tcp://127.0.0.1:6666");
        ZMQ.Context context2 = ZMQ.context(2);
        ZMQ.Socket publisher2 = context2.socket(ZMQ.PUB);
        publisher2.bind("tcp://127.0.0.1:6667");
        ZMQ.Context context3 = ZMQ.context(3);
        ZMQ.Socket subscriber = context3.socket(ZMQ.SUB);
        subscriber.bind("tcp://127.0.0.1:6668");

        Map<String,ActorRef> empresasToActors = new HashMap<String,ActorRef>();
        ActorRef exchange = new ExchangeInstance(empresasToActors).spawn();
        new CheckTransactions(subscriber,exchange).start();
        for (int i=0; i<empresas.size();i++){
            empresasToActors.put(empresas.get(i),new CompanyOrders(empresas.get(i),exchange,publisher,publisher2).spawn());
        }
        Acceptor acceptor = new Acceptor(port, exchange);
        acceptor.spawn();
        acceptor.join();
    }
}
