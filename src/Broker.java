package com.example.opalakia;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

public class Broker extends Node implements Runnable, Serializable {

    private ArrayList <String> artists = new ArrayList<>();
    private HashMap<String, Node> artistNames = new HashMap<>();
    private HashMap <String, Node> respBroker = new HashMap<>();
    private ArrayList <Node> brokerList = new ArrayList<>();
    private ArrayList <Node> publishers = new ArrayList<>();
    private ArrayList <Node> consumers = new ArrayList<>();
    private HashMap <BigInteger, Node> hashKeys = new HashMap<>();
    private ArrayList<BigInteger> hashes = new ArrayList<>();
    private Socket socket = null;
    private String connectionType = "";

    public Broker(String ipAddress, int port) {
        super(ipAddress, port);
        createBrokerList();
        calculateKeys();
    }

    public Broker (Broker temp) {
        super(temp.getIpAddress(),temp.getPort());
        this.artists = temp.artists;
        this.artistNames = temp.artistNames;
        this.respBroker = temp.respBroker;
        this.brokerList = temp.brokerList;
        this.publishers = temp.publishers;
        this.consumers = temp.consumers;
        this.hashKeys = temp.hashKeys;
        this.hashes = temp.hashes;
        this.socket = temp.socket;
        this.connectionType = temp.connectionType;
    }

    private void createBrokerList() {
        File f = null;
        BufferedReader reader = null;
        String line;
        try{
            f = new File("BrokerList.txt");
        }
        catch (NullPointerException e){
            System.err.println ("File not found.");
        }
        try{
            reader = new BufferedReader(new FileReader(f));
        }
        catch (FileNotFoundException e ){
            System.err.println("Error opening file!");
        }
        try {
            line = reader.readLine();
            while (line!= null) {
                line = line.trim();
                StringTokenizer token = new StringTokenizer(line, " ");
                brokerList.add(new Node(token.nextToken(), Integer.parseInt(token.nextToken())));
                line = reader.readLine();
            }
        }
        catch (IOException e) {
            System.err.println("Error while reading the file.");
        }
        try {
            reader.close();
        } catch (IOException e) {
            System.err.println("Error closing file.");
        }
    }

    private void calculateKeys(){
        for (Node node : brokerList){
            BigInteger temp = Hash_with_MD5.Hash(node.getIpAddress(),node.getPort());
            hashKeys.putIfAbsent(temp, node);
            if (!hashes.contains(temp)) hashes.add(temp);
        }
        hashes.sort(null);
    }


    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void setConnectionType(String connectionType) {
        this.connectionType = connectionType;
    }

    public void startPServer() {
        ServerSocket listenerSocket = null;
        Socket connection=null;
        ArrayList<Thread> threads = new ArrayList();
        try {
            listenerSocket = new ServerSocket(port);
            while (true) {
                System.out.println("Server for publishers is up and waiting ");
                connection = listenerSocket.accept();
                Broker temp = new Broker(this);
                temp.setSocket(connection);
                Thread t = new Thread( temp );
                t.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = threads.size()-1;i>-1;i--){
            if (!threads.get(i).isAlive()){
                try {
                    threads.get(i).join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                threads.remove(i);
            }
        }
    }

    public void startCServer() {
        ServerSocket listenerSocket = null;
        Socket connection=null;
        ArrayList<Thread> threads = new ArrayList();
        try {
            listenerSocket = new ServerSocket(port+1);
            while (true) {
                System.out.println("Server for consumers is up and waiting ");
                connection = listenerSocket.accept();
                Broker temp = new Broker(this);
                temp.setSocket(connection);
                Thread t = new Thread( temp );
                t.start();
                threads.add(t);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = threads.size()-1;i>-1;i--){
            if (!threads.get(i).isAlive()){
                try {
                    threads.get(i).join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                threads.remove(i);
            }
        }
    }

    @Override
    public void run() {
        if (connectionType.equalsIgnoreCase("Consumer") ){
            if (socket!=null){
                acceptConnectionCons(socket);
            }
            else {startCServer();}
        }
        else {
            if (socket!=null){
                acceptConnectionPubl(socket);
            }
            else {startPServer();}
        }
    }

    private void acceptConnectionCons (Socket socket) {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        Socket socket1 = null;
        ObjectOutputStream out1 = null;
        ObjectInputStream in1 = null;
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            out.writeUTF("Broker: Connection Successful ");
            out.flush();
            String consumer = socket.getInetAddress().getHostAddress();
            System.out.println("Connected consumer : " + consumer);
            String request = in.readUTF();
            System.out.println("Request for: " + request + " from --> " + consumer);
            if (request.equalsIgnoreCase("Checking connection")){
                out.writeUTF("Connection established");
                out.flush();
            }
            else if (request.equalsIgnoreCase("I want a song")) {
                boolean foundArt = false;
                boolean resp = false;
                String artistName = null;
                while (!foundArt) {
                    out.writeUTF("What is the name of the artist?");
                    out.flush();
                    out.writeObject(artists);
                    out.flush();
                    artistName = in.readUTF();  // mas stelnei to artist name
                    foundArt = artists.contains(artistName);
                    out.writeBoolean(foundArt);
                    out.flush();
                }
                resp= artistNames.containsKey(artistName);
                out.writeBoolean(resp);
                out.flush();
                if (!resp) {
                    out.writeObject(respBroker.get(artistName));
                    out.flush();
                } else {
                    Node publisher = artistNames.get(artistName);
                    boolean foundSong = false;
                    String songTitle = null;
                    while (!foundSong){
                        socket1 = connect(publisher.getIpAddress(),publisher.getPort());
                        out1 = new ObjectOutputStream(socket1.getOutputStream());
                        in1 = new ObjectInputStream(socket1.getInputStream());
                        System.out.println(in1.readUTF() );
                        out1.writeUTF("asking song");
                        out1.flush();
                        out1.writeUTF(artistName);
                        out1.flush();
                        boolean correctPubl = in1.readBoolean();
                        if (correctPubl){
                            out.writeUTF("What song do you want?");
                            out.flush();
                            ArrayList<String> songs = (ArrayList<String>) in1.readObject();
                            out.writeObject(songs);
                            out.flush();
                            songTitle = in.readUTF();
                            out1.writeUTF( songTitle );
                            out1.flush();
                            foundSong = in1.readBoolean();
                            out.writeBoolean(foundSong);
                            out.flush();
                            if (!foundSong){
                                out.writeUTF("What song do you want?");
                                out.flush();
                                songs = (ArrayList) in1.readObject();
                                out.writeObject(songs);
                                out.flush();
                            }
                            else {
                                int chunkSize = in1.readInt();
                                out.writeInt(chunkSize);
                                out.flush();
                                String ok = in.readUTF();
                                System.err.println(ok);
                                out1.writeUTF(ok);
                                out1.flush();
                                //write code for failure
                                for (int i = 0;i<chunkSize;i++){
                                    Value chunk = (Value) in1.readObject();
                                    out.writeObject(chunk);
                                    out.flush();
                                    ok = in.readUTF();
                                    out1.writeUTF(ok);
                                    out1.flush();
                                }
                                System.out.println("Publisher: " + in1.readUTF());
                            }
                            out1.close();
                            in1.close();
                            disconnect(socket1);
                        }
                        else { System.err.println("Problem with list of Publishers"); }
                    }
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        finally{
            try {
                if (in!=null) in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (out!=null) out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try{
                if (socket!=null) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if(in1!=null) in1.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (out1!=null) out1.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try{
                if (socket1!=null) socket1.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void acceptConnectionPubl (Socket socket) {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            out.writeUTF("Broker: Connection Successful ");
            out.flush();
            String publisher = socket.getInetAddress().getHostAddress();
            System.out.println("Connected publisher : " + publisher);
            String request = in.readUTF();
            System.out.println("Request for: " + request + " from --> " + publisher);
            if (request.equalsIgnoreCase("brokerList")) {
                out.writeObject(brokerList);
                out.flush();
            } else if (request.equalsIgnoreCase("push")) {
                Node temp = (Node) in.readObject();
                ArrayList <String> artists = (ArrayList) in.readObject();
                arrangeArtists (artists, temp);
            }
            } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        finally{
            try {
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try{
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void arrangeArtists (ArrayList<String> artistsList, Node publisher){
        for (String artistName: artistsList){
            BigInteger hashArtist = Hash_with_MD5.Hash(artistName);
            hashArtist = hashArtist.mod(hashes.get(hashes.size()-1));
            if (!artists.contains(artistName)) artists.add(artistName);
            for (BigInteger hash : hashes) {
                if ( hashArtist.compareTo(hash) < 0 ){
                    Node node = hashKeys.get(hash);
                    if ( node.getIpAddress().equals(getIpAddress()) && node.getPort() == getPort() ){
                        artistNames.putIfAbsent(artistName, publisher);
                    }
                    else {
                        respBroker.putIfAbsent(artistName,node);
                    }
                    break;
                }
            }
        }
    }



    public static void main(String[] args) throws InterruptedException {
        //Broker b1 = new Broker("192.168.2.2",4251);
        //Broker b1 = new Broker("192.168.2.2", 5786);
        Broker b1 = new Broker("192.168.2.2",4785);
        Broker temp = new Broker(b1);
        temp.setConnectionType("Publisher");
        Thread t1 = new Thread(new Broker(temp));
        temp.setConnectionType("Consumer");
        Thread t2  = new Thread (new Broker(temp));
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

}