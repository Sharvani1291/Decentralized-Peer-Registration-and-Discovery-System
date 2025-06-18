import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.net.*;
import java.io.IOException;

public class BootstrapServer {
    public static Socket socket = null;
    public static DataInputStream dataInputStream = null;
    public static DataOutputStream dataOutputStream = null;
    public static int bPortNumber = 0;
    public static int bId = 0;
    public static int capacity = 1024;
    public static String workingDir = System.getProperty("user.dir");
    public static HashTable hTable;
    public static Table table;

    public static void main(String[] args) {

        int lines, key;
        String value;
        String[] pArr;

        File bnconfig = new File(workingDir + File.separator + args[0]);

        if (!bnconfig.exists()) {
            System.out.println("Configuration file does not exists!!!");
            return;
        }

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(bnconfig));
            lines = (int) Files.lines(Paths.get(workingDir + File.separator + args[0])).count();
            bId = Integer.parseInt(bufferedReader.readLine().trim());
            bPortNumber = Integer.parseInt((bufferedReader.readLine().trim()));
            hTable = new HashTable();
            for (int i = 2; i < lines; i++) {
                pArr = bufferedReader.readLine().split("\\s+");
                key = Integer.parseInt(pArr[0]);
                value = pArr[1];
                if(key >= 0 && key < capacity) {
                    hTable.add(key, value);
                }else {
                    System.out.println(key + " is not a valid key");
                }
            }
            hTable.print();

            System.out.println("BootStrap Server Started");
            ServerSocket serverSocket = new ServerSocket(bPortNumber);

            InetAddress inetAddress = InetAddress.getLocalHost();
            String currentIP = inetAddress.getHostAddress().trim();
            System.out.println("Current IP :"+inetAddress.getHostAddress());
            table = new Table(bPortNumber,bPortNumber,bPortNumber,hTable.next(bId), bId, hTable, bPortNumber,currentIP,currentIP,currentIP,null,null);
            table.print();

            BootstrapServerThread bstrapThread = new BootstrapServerThread();
            new Thread(bstrapThread).start();

            ServerConnectionsThread serverConnectionsThread = new ServerConnectionsThread(serverSocket);
            new Thread(serverConnectionsThread).start();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class BootstrapServerThread implements Runnable {
    Scanner sc = new Scanner(System.in);

    @Override
    public void run() {
        while (true) {
            System.out.print("> ");
            String cmd = sc.nextLine().trim();
            System.out.println("Current working thread: " + Thread.currentThread().getName());
            String[] parts = cmd.split("\\s+");
            String command = parts[0];
            switch (command) {
                case "lookup":
                    if (parts.length != 2) {
                        System.out.println("Inappropriate command");
                        return;
                    }
                    lookup(parts[1]);
                    break;
                case "Insert":
                    if (parts.length != 3) {
                        System.out.println("Inappropriate command");
                        return;
                    }
                    if (Integer.parseInt(parts[1]) < 0 || Integer.parseInt(parts[1]) > 1023) {
                        System.out.println("Invalid Key");
                        return;
                    }
                    try {
                        Insert(parts[1], parts[2]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case "delete":
                    if (parts.length != 2) {
                        System.out.println("Inappropriate command");
                        return;
                    }
                    delete(parts[1]);
                    break;
                default:
                    break;
            }
        }
    }

        private void lookup(String k) {
        int key = Integer.parseInt(k);
        if(BootstrapServer.hTable.check(key)){
            System.out.println(BootstrapServer.hTable.getValue(key));
            BootstrapServer.hTable.print();
        }
        else {
            System.out.println("Key not found");
        }
    }

    private void Insert(String k, String value) throws IOException {
        int key = Integer.parseInt(k);

        if(!BootstrapServer.hTable.check(key)) {
            if (BootstrapServer.hTable.belongs(key)) {
                BootstrapServer.hTable.add(key, value);
            }
        }
        else{
            System.out.println("Key already exists");
        }
    }

    private void delete(String k) {
        int key = Integer.parseInt(k);
        if(BootstrapServer.hTable.check(key)){
            BootstrapServer.hTable.remove(key);
        }
        else {
            System.out.println("key not exists");
        }
    }
}

class ServerConnectionsThread implements Runnable {

    ServerSocket serverSocket;
    Socket socket;

    public ServerConnectionsThread(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
    }

    public void run() {
        while (true) {
            try {
                socket = serverSocket.accept();
                System.out.println("A Socket accepted");
                BootstrapServer.table.suc_socket = socket;
                DivisionThread divisionThread = new DivisionThread(socket);
                new Thread(divisionThread).start();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}

class DivisionThread implements Runnable {

    Socket socket;
    ServerSocket serverSocket;
    DataInputStream dataInputStream = BootstrapServer.dataInputStream;
    DataOutputStream dataOutputStream = BootstrapServer.dataOutputStream;
    int nId, new_prePort, new_sucPort;
    String new_pre_IP, new_suc_IP;
    HashTable  new_hTable;

    public DivisionThread(Socket socket) {
        this.socket = socket;
    }

    public void run() {
        try {
            if (socket != null) {
                System.out.println("A Server Added");
                dataInputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                dataOutputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

                //dataOutputStream.writeInt(BootstrapServer.table.key_start);

                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                //ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                dataOutputStream.writeUTF("Division");
                System.out.println("A");
                nId = dataInputStream.readInt();
                System.out.println("Server " + nId + " Connected");

                if (nId == 0 || (nId>BootstrapServer.table.key_start && nId < 1024)) {
                    dataOutputStream.writeUTF("YES");
                    dataOutputStream.flush();
                    new_hTable = BootstrapServer.hTable.divide(BootstrapServer.hTable.next(BootstrapServer.table.key_end), nId);
                    oos.writeObject(new_hTable);
                    oos.flush();
                    dataOutputStream.writeInt(BootstrapServer.table.suc_portNumber);
                    dataOutputStream.writeInt(BootstrapServer.hTable.next(BootstrapServer.table.key_end));
                    dataOutputStream.writeUTF(BootstrapServer.table.curr_IP);
                    dataOutputStream.writeUTF(BootstrapServer.table.suc_IP);
                    dataOutputStream.flush();

                    new_sucPort = dataInputStream.readInt();
                    BootstrapServer.table.suc_IP = dataInputStream.readUTF();
                    BootstrapServer.table.suc_portNumber = new_sucPort;
                    BootstrapServer.table.key_start = BootstrapServer.hTable.next(nId);
                    BootstrapServer.hTable = BootstrapServer.hTable.update();
                    BootstrapServer.table.hTable = BootstrapServer.hTable.update();

                    BootstrapServer.table.pre_portNumber = dataInputStream.readInt();
                    BootstrapServer.table.pre_IP = dataInputStream.readUTF();

                    BootstrapServer.table.print();
                    System.out.println("Server " + nId + " added and Partition Completed");

                    OperationThread operationThread = new OperationThread();
                    new Thread(operationThread).start();

                }
                else{
                    dataOutputStream.writeUTF("NO");
                    dataOutputStream.flush();
                    dataOutputStream.writeInt(BootstrapServer.table.suc_portNumber);
                    dataOutputStream.writeUTF(BootstrapServer.table.suc_IP);
                    dataOutputStream.flush();
                }
            }
        }catch(IOException e){
                throw new RuntimeException(e);
            }
    }
}

class OperationThread implements Runnable {

    Scanner sc = new Scanner(System.in);
    Socket socket;
    DataInputStream dataInputStream;
    DataOutputStream dataOutputStream;
    String cmd;

    @Override
    public void run() {
        while (true) {

            try {
                socket = new Socket(BootstrapServer.table.suc_IP, BootstrapServer.table.suc_portNumber);

                dataInputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                dataOutputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

                dataOutputStream.writeUTF("Operations");

                System.out.print("bstrap > ");
                cmd = sc.nextLine().trim();
                System.out.println("Current working thread: " + Thread.currentThread().getName());
                String[] parts = cmd.split("\\s+");
                String command = parts[0];
                switch (command) {
                    case "lookup":
                        if (parts.length != 2) {
                            System.out.println("Inappropriate command");
                            return;
                        }
                        try {
                            lookup(parts[1]);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "Insert":
                        if (parts.length != 3) {
                            System.out.println("Inappropriate command");
                            return;
                        }
                        if (Integer.parseInt(parts[1]) < 0 || Integer.parseInt(parts[1]) > 1023) {
                            System.out.println("Invalid Key");
                            return;
                        }
                        try {
                            Insert(parts[1], parts[2]);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "delete":
                        if (parts.length != 2) {
                            System.out.println("Inappropriate command");
                            return;
                        }
                        try {
                            delete(parts[1]);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    default:
                        break;
                }
            }catch (IOException e) {
                    throw new RuntimeException(e);
                }
    }
    }

    private void lookup(String k) throws IOException {
        int key = Integer.parseInt(k);
        if(BootstrapServer.hTable.check(key)){
            System.out.println(BootstrapServer.hTable.getValue(key));
            BootstrapServer.hTable.print();
        }
        else {
            if(key == 0 || (key>BootstrapServer.table.key_start && key < 1024)){
                System.out.println("Key not found");
            }
            else {
                dataOutputStream.writeUTF(cmd);
                dataOutputStream.flush();
            }
        }
    }

    private void Insert(String k, String value) throws IOException {
        int key = Integer.parseInt(k);

        if(BootstrapServer.hTable.check(key)) {
            System.out.println("Key already exists");
        }
        else{
            if(key<-1 || key>1023) {
                System.out.println("Invalid Key");
            }
            else if (key == 0 || (key>BootstrapServer.table.key_start && key < 1024)) {
                BootstrapServer.hTable.add(key, value);
            }
            else{
                dataOutputStream.writeUTF(cmd);
                dataOutputStream.flush();
            }
        }
    }

    private void delete(String k) throws IOException {
        int key = Integer.parseInt(k);
        if(BootstrapServer.hTable.check(key)){
            BootstrapServer.hTable.remove(key);
        }
        else {
            if(key<-1 || key>1023) {
                System.out.println("Invalid Key");
            }
            else if (key == 0 || (key>BootstrapServer.table.key_start && key < 1024)) {
                System.out.println("key not exists");
            }
            else{
                dataOutputStream.writeUTF(cmd);
                dataOutputStream.flush();
            }
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

class Pair implements Serializable {
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
    boolean belongs(int key){
            if(keySet.getFirst()<key && key<keySet.getLast()){
                return true;
            }
        return false;
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