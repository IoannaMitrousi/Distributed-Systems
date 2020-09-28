package com.example.opalakia;

import org.jetbrains.annotations.NotNull;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

public class Consumer extends Node {

    private String brokerIP;
    private int brokerPort;

    public Consumer(String ipAddress, int port) {
        super(ipAddress, port);
    }

    public Consumer(String ipAddress, int port, String brokerIP, int brokerPort) {
        super(ipAddress, port);
        this.brokerIP = brokerIP;
        this.brokerPort = brokerPort;
    }

    public String getBrokerIP() {
        return brokerIP;
    }

    public void setBrokerIP(String brokerIP) {
        this.brokerIP = brokerIP;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(int brokerPort) {
        this.brokerPort = brokerPort+1;
    }

    public File mergeFile(@NotNull ArrayList<Value> chunks) throws IOException {
        File file = new File(chunks.get(0).getFilename());
        try (FileOutputStream out = new FileOutputStream(file)) {
            for (Value chunk: chunks) {
                byte[] buffer = chunk.getData();
                out.write(buffer);
            }
            out.close();
        }
        return file;
    }

    public void makeConnection() {
        boolean foundArtist = false;  //Auto mas deixnei an exoume epileksei artist o opoios iparxei sti lista oste na min xreiastei na diavasoume artistName ksana
        String artistName = null;
        Scanner in1 = new Scanner(System.in);
        ObjectInputStream in =null;
        ObjectOutputStream out = null;
        boolean correctBr = false;
        Socket socket = null;
        try {
            while (!correctBr) {
                socket = connect(brokerIP, brokerPort);
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());
                System.out.println(in.readUTF());
                out.writeUTF("I want a song");
                out.flush();
                System.out.println(in.readUTF());
                ArrayList <String> artists = (ArrayList <String>) in.readObject();
                if (!foundArtist) {artistName = in1.nextLine();}
                else {System.out.println(artistName);}
                out.writeUTF(artistName);
                out.flush();
                boolean correct = in.readBoolean();
                while (!correct) {
                    System.out.println("Wrong Artist");
                    System.out.println(in.readUTF());
                    artistName = in1.nextLine();
                    out.writeUTF(artistName);
                    out.flush();
                    correct = in.readBoolean();
                }
                foundArtist = true;
                correctBr = in.readBoolean();
                if (!correctBr) {
                    Node broker = (Node) in.readObject();
                    setBrokerIP(broker.getIpAddress());
                    setBrokerPort(broker.getPort());
                    out.close();
                    in.close();
                    disconnect(socket);
                }
            }
            System.out.println(in.readUTF());
            ArrayList<String> songs = (ArrayList) in.readObject();
            out.writeUTF(in1.nextLine());
            out.flush();
            boolean correctSong = in.readBoolean();
            while (!correctSong){
                System.out.println(in.readUTF());
                songs = (ArrayList) in.readObject();
                System.out.println("The song you asked doesn't exist. Here is a list of the songs of the artist you asked");
                for (String song : songs){
                    System.out.println(song);
                }
                out.writeUTF(in1.nextLine());
                out.flush();
                correctSong = in.readBoolean();
            }
            int chunkNumber = in.readInt();
            ArrayList <Value> chunks = new ArrayList();
            for (int i =0;i<chunkNumber;i++){
                Value chunk = (Value) in.readObject();
                chunks.add(chunk);
            }
            mergeFile(chunks);
            out.close();
            in.close();
            disconnect(socket);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {
        Consumer cons1 = new Consumer ("192.168.1.15", 8542);
        cons1.setBrokerIP ("192.168.1.15");
        cons1.setBrokerPort(4251);
        cons1.makeConnection();
    }

}