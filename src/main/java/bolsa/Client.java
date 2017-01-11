package bolsa;

import com.google.protobuf.CodedOutputStream;

import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
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
            e.printStackTrace();
        }
    }
    
}
class Send_Data extends Thread{
    private Socket connection;
    public Send_Data (Socket connection){
        this.connection=connection;
    }


    public void run() {
        try{
            Scanner in = new Scanner(System.in);
            System.out.println("Por favor introduza o seu utilizador:");
            String username = in.next();
            System.out.println("Por favor introduza a sua password:");
            String password = in.next();
            CodedOutputStream cos = CodedOutputStream.newInstance(connection.getOutputStream());
            Utilizador.User usercreation = createUser(username,password);
            byte[]user_byte = usercreation.toByteArray();
            cos.writeInt32NoTag(user_byte.length);
            cos.writeRawBytes(user_byte);
            cos.flush();
        } catch(Exception e) {e.printStackTrace();}


        BufferedReader inputFromConsole = new BufferedReader(new InputStreamReader(System.in));
        try {
            PrintWriter writer = new PrintWriter(this.connection.getOutputStream());
            String buffer;
            while ((buffer = inputFromConsole.readLine()) != null) {
                writer.println(buffer);
                writer.flush();
            }
        }
        catch (Exception e) {e.printStackTrace();}
    }

    static Utilizador.User createUser(String username,String password){
        return
                Utilizador.User.newBuilder()
                .setUsername(username)
                .setPassword(password)
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