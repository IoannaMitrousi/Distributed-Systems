package com.example.opalakia;
import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class Node implements Serializable {
    private static final long serialVersionUID = -3643274596837043061L;
    public static ArrayList <String> brokers = new ArrayList();
    protected String ipAddress;
    protected int port;

    public Node() {
    }

    public Node(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void init(int temp) {

    }
    public ArrayList<String> getBrokers() {
        return brokers;
    }



    public  Socket connect(String ip , int port){
        while (true){
            try {
                InetAddress host = Inet4Address.getByName(ip);
                Socket socket = new Socket(host,port);
                return socket;
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println(e);
            }
        }
    }

    public void disconnect(Socket socket){
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateNodes(){};
}
