import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.*;

public class NormalServer {
    public static Socket socket = null;
    public static DataInputStream dataInputStream;
    public static DataOutputStream dataOutputStream;
    public static int bPortNumber,portNumber;
    public static int capacity = 1024;
    public static String workingDir = System.getProperty("user.dir");
    public static HashTable hTable = new HashTable();
    public static Table table;

    public static void main(String[] args) {

        int id;
        String bHost;
        String[] Net;

        File bnconfig = new File(workingDir + File.separator + args[0]);

        if (!bnconfig.exists()) {
            System.out.println("Configuration file does not exists!!!");
            return;
        }

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(bnconfig));
            id = Integer.parseInt(bufferedReader.readLine().trim());
            NormalServer.portNumber = Integer.parseInt((bufferedReader.readLine().trim()));
            Net = bufferedReader.readLine().split("\\s+");
            bHost = Net[0];
            NormalServer.bPortNumber = Integer.parseInt(Net[1]);

            InetAddress inetAddress = InetAddress.getLocalHost();
            String currentIP = inetAddress.getHostAddress().trim();
            System.out.println(inetAddress.getHostAddress());

            NormalServer.table = new Table(NormalServer.portNumber,0, 0, 0, id, NormalServer.hTable, NormalServer.bPortNumber,null,null,currentIP,null,null);

            UserThread userThread = new UserThread(id, bHost,portNumber);
            new Thread(userThread).start();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}

class UserThread implements Runnable {
    int id, portNumber;
    int bPortNumber = NormalServer.bPortNumber;
    String bHost;
    Socket socket = NormalServer.socket;
    Scanner sc = new Scanner(System.in);

    public UserThread(int id, String bHost, int portNumber) {
        this.id = id;
        this.bHost = bHost;
        this.portNumber = portNumber;
    }

    @Override
    public void run() {
        while (true) {
            System.out.print("nServer " + id + " > ");
            String cmd = sc.nextLine().trim();
            System.out.println("Current working thread: " + Thread.currentThread().getName());
            switch (cmd) {
                case "enter":
                    try {
                        enter();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case "exit":
                    try {
                        exit();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    System.out.println("Invalid command");
                    break;
            }
        }
    }

    private void enter() throws IOException {
        System.out.println("Normal Server "+id+" Started");

        socket = new Socket(bHost, bPortNumber);
        System.out.println("BootStrap Server Connected");


        EnterThread enterThread = new EnterThread(socket,portNumber);
        new Thread(enterThread).start();


    }


    private void exit() throws IOException {
        ExitThread exitThread = new ExitThread();
        new Thread(exitThread).start();
    }

}

class EnterThread implements Runnable {

    Socket socket;
    int portNumber;

    public EnterThread(Socket socket, int portNumber) {
        this.socket = socket;
        this.portNumber = portNumber;
    }

    @Override
    public void run() {

        DivisionThread divisionThread = new DivisionThread(socket,portNumber);
        new Thread(divisionThread).start();

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(portNumber);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ConnectionsThread connectionsThread = new ConnectionsThread(serverSocket,portNumber);
        new Thread(connectionsThread).start();
    }
}

class ExitThread implements Runnable {

    @Override
    public void run() {

    }
}

class DivisionThread implements Runnable {
    int portNumber, new_keyStart, new_keyEnd,new_prePort, new_sucPort;
    String new_sucIP,new_preIP;
    Socket socket;
    DataOutputStream dataOutputStream = NormalServer.dataOutputStream;
    DataInputStream dataInputStream = NormalServer.dataInputStream;
    HashTable new_hTable;

    DivisionThread(Socket socket, int portNumber) {
        this.socket = socket;
        this.portNumber = portNumber;
    }

    @Override
    public void run() {
        try {

            int id = NormalServer.table.key_end;
            dataInputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            dataOutputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            //ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

            System.out.println("A");
            dataOutputStream.writeInt(id);
            dataOutputStream.flush();
            if (dataInputStream.readUTF().equals("YES")) {
                System.out.println("B");
                new_hTable = (HashTable) ois.readObject();
                NormalServer.hTable = new_hTable;
                System.out.println("F");
                for (int k : NormalServer.hTable.keySet) {
                    System.out.println(k + "   " + NormalServer.hTable.getValue(k));
                }
                NormalServer.table.suc_portNumber = dataInputStream.readInt();
                System.out.println("C");
                NormalServer.table.key_start = dataInputStream.readInt();
                System.out.println("D");
                NormalServer.table.pre_IP = dataInputStream.readUTF();
                NormalServer.table.suc_IP = dataInputStream.readUTF();
                System.out.println("E");
                dataOutputStream.writeInt(portNumber);
                dataOutputStream.writeUTF(NormalServer.table.curr_IP);
                dataOutputStream.flush();

                NormalServer.table.pre_portNumber = socket.getPort();
                NormalServer.table.hTable = NormalServer.hTable;
                NormalServer.table.print();
                if(NormalServer.table.suc_portNumber == NormalServer.bPortNumber){
                    dataOutputStream.writeInt(portNumber);
                    dataOutputStream.writeUTF(NormalServer.table.curr_IP);
                    dataOutputStream.flush();
                }
                else {
                    Socket socket1 = new Socket(NormalServer.table.suc_IP,NormalServer.table.suc_portNumber);
                    DataOutputStream dataOutputStream1 = new DataOutputStream(new BufferedOutputStream(socket1.getOutputStream()));
                    dataOutputStream1.writeInt(portNumber);
                    dataOutputStream1.writeUTF(NormalServer.table.curr_IP);
                    dataOutputStream1.flush();
                }
            }
            else{
                new_sucPort = dataInputStream.readInt();
                new_sucIP = dataInputStream.readUTF();

                Socket socket1 = new Socket(new_sucIP,new_sucPort);

                EnterThread enterThread = new EnterThread(socket1,new_sucPort);
                new Thread(enterThread).start();
            }

        }catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }
}

class ConnectionsThread implements Runnable {

    Socket socket;
    ServerSocket serverSocket;
    int nId, new_prePort, new_sucPort, portNumber;
    String new_pre_IP, new_suc_IP;
    HashTable new_hTable;

    public ConnectionsThread(ServerSocket serverSocket, int portNumber) {
        this.serverSocket = serverSocket;
        this.portNumber = portNumber;
    }

    @Override
    public void run() {

        try{
            while(true){
                socket = serverSocket.accept();
                System.out.println("A Server attached to "+socket.getPort());

                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

                    if (socket != null) {
                        System.out.println("A Server Added");


                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

                        if (dataInputStream.readUTF().equals("Division")) {
                            System.out.println("A");
                            nId = dataInputStream.readInt();
                            System.out.println("Server " + nId + " Connected");

                            if (nId > NormalServer.table.key_start && nId < NormalServer.table.key_end) {
                                dataOutputStream.writeUTF("YES");
                                dataOutputStream.flush();
                                new_hTable = NormalServer.hTable.divide(NormalServer.hTable.next(NormalServer.table.key_end), nId);
                                oos.writeObject(new_hTable);
                                oos.flush();
                                dataOutputStream.writeInt(NormalServer.table.suc_portNumber);
                                dataOutputStream.writeInt(NormalServer.hTable.next(NormalServer.table.key_end));
                                dataOutputStream.writeUTF(NormalServer.table.curr_IP);
                                dataOutputStream.writeUTF(NormalServer.table.suc_IP);
                                dataOutputStream.flush();

                                new_sucPort = dataInputStream.readInt();
                                NormalServer.table.suc_IP = dataInputStream.readUTF();
                                NormalServer.table.suc_portNumber = new_sucPort;
                                NormalServer.table.key_start = NormalServer.hTable.next(nId);
                                NormalServer.table.hTable = NormalServer.hTable.update();

                                if (NormalServer.table.suc_portNumber == NormalServer.bPortNumber) {
                                    NormalServer.table.pre_portNumber = dataInputStream.readInt();
                                    NormalServer.table.pre_IP = dataInputStream.readUTF();
                                } else {
                                    ServerSocket serverSocket1 = new ServerSocket(NormalServer.table.suc_portNumber);
                                    Socket socket1 = serverSocket1.accept();
                                    DataInputStream dataInputStream1 = new DataInputStream(socket1.getInputStream());
                                    NormalServer.table.pre_portNumber = dataInputStream1.readInt();
                                    NormalServer.table.pre_IP = dataInputStream1.readUTF();
                                }

                                NormalServer.table.print();
                                System.out.println("Server " + nId + " added and Partition Completed");

                                OperationThread operationThread = new OperationThread();
                                new Thread(operationThread).start();

                            } else {
                                dataOutputStream.writeUTF("NO");
                                dataOutputStream.flush();
                                dataOutputStream.writeInt(NormalServer.table.suc_portNumber);
                                dataOutputStream.writeUTF(NormalServer.table.suc_IP);
                                dataOutputStream.flush();
                            }

                            DivisionThread divisionThread = new DivisionThread(socket, portNumber);
                            new Thread(divisionThread).start();
                        }
                        else{
                            System.out.println(dataInputStream.readUTF());
                        }
                    }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class OperationThread implements Runnable {

    Socket socket;
    ServerSocket serverSocket;
    DataInputStream dataInputStream;
    DataOutputStream dataOutputStream;
    int nId, new_prePort, new_sucPort, bPortNumber, portNumber;
    HashTable hTable, new_hTable;
    Table table;
    String cmd;


    public void run() {
            try {
                serverSocket = new ServerSocket(NormalServer.table.curr_portNumber);
                socket = serverSocket.accept();
                dataInputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                dataOutputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));


                System.out.println(dataInputStream.readUTF());


            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

class Table {
    int pre_portNumber,suc_portNumber, key_start,key_end, boot_portNumber, curr_portNumber;
    String pre_IP, suc_IP,curr_IP;
    HashTable hTable;
    Socket pre_socket, suc_socket;

    public Table(int curr_portNumber, int pre_portNumber, int suc_portNumber, int key_start, int key_end, HashTable hTable, int boot_portNumber, String pre_IP, String suc_IP, String curr_IP, Socket pre_socket, Socket suc_socket) {
        this.curr_portNumber = curr_portNumber;
        this.pre_portNumber = pre_portNumber;
        this.suc_portNumber = suc_portNumber;
        this.key_start = key_start;
        this.key_end = key_end;
        this.hTable = hTable;
        this.boot_portNumber = boot_portNumber;
        this.pre_IP = pre_IP;
        this.suc_IP = suc_IP;
        this.curr_IP = curr_IP;
        this.pre_socket = pre_socket;
        this.suc_socket = suc_socket;
    }
    void print(){
        System.out.println("curr_portNumber: " + curr_portNumber);
        System.out.println("pre_portNumber: "+pre_portNumber);
        System.out.println("suc_portNumber: "+suc_portNumber);
        System.out.println("key_start: "+key_start);
        System.out.println("key_end: "+key_end);
        System.out.println("boot_portNumber: "+boot_portNumber);
        System.out.println("hTable: "+hTable);
        System.out.println("pre_IP: "+pre_IP);
        System.out.println("suc_IP: "+suc_IP);
        System.out.println("curr_IP: "+curr_IP);
        System.out.println("pre_socket: "+pre_socket);
        System.out.println("suc_socket: "+suc_socket);
    }
}

class Pair implements Serializable{
    int key;
    String value;

    Pair(int key, String value) {
        this.key = key;
        this.value = value;
    }
}

class HashTable implements Serializable{
    List<Pair> hTable;
    List<Integer> keySet;
    @Serial
    private static final long serialVersionUID = 7977107694701101293L;

    HashTable(){
        hTable = new ArrayList<>();
        keySet = new ArrayList<>();
    }

    void add(int key, String value){
        hTable.add(new Pair(key, value));
        keySet.add(key);
        hTable.sort(Comparator.comparing(p -> p.key));
        keySet.sort(Comparator.naturalOrder());
    }

    void remove(int key){
        hTable.removeIf(k -> k.key == key);
        keySet.remove((Integer) key);
    }

    void modify(int key, String value){
        hTable.get(key).value = value;
    }

    int next(int key){
        if(key == 1023){
            return 0;
        }
        else{
            return key+1;
        }
    }

    void print(){
        hTable.forEach(p -> System.out.println("key - "+p.key + ": value = "+ p.value));
        System.out.println("Size = "+hTable.size());
    }

    int size(){
        return keySet.size();
    }

    boolean check(int key){
        for(int k : keySet){
            if(k == key){
                return true;
            }
        }
        return false;
    }

    String getValue(int key){
        for(Pair p : hTable){
            if(p.key == key){
                return p.value;
            }
        }
        return "key not found";
    }

    HashTable divide(int key1, int key2){
        HashTable hTable1 = new HashTable();
        Iterator<Pair> iterator = hTable.iterator();
        while (iterator.hasNext()) {
            Pair p = iterator.next();
            if (key1 < p.key && p.key <= key2) {
                hTable1.add(p.key, p.value);
                iterator.remove();
            }
        }
        hTable1.print();
        return hTable1;
    }
    HashTable update(){
        hTable.sort(Comparator.comparing(p -> p.key));
        keySet.sort(Comparator.naturalOrder());
        HashTable new_hTable = new HashTable();
        List<Integer> keySet1 = new ArrayList<>();
        for(Pair p : hTable){
            new_hTable.add(p.key, p.value);
            keySet1.add(p.key);
        }
        keySet = keySet1;
        new_hTable.print();
        return new_hTable;
    }
}