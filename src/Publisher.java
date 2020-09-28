package com.example.opalakia;
import com.mpatric.mp3agic.ID3v2;
import com.mpatric.mp3agic.InvalidDataException;
import com.mpatric.mp3agic.Mp3File;
import com.mpatric.mp3agic.UnsupportedTagException;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;


public class Publisher extends Node implements Runnable, Serializable {

    private String brokerIp;
    private int brokerPort;
    private String startLetter;
    private String finishLetter;
    private ArrayList<String> artistNames = new ArrayList();
    private ArrayList <ArrayList<File>> musicFiles = new ArrayList();
    private ArrayList <ArrayList<String>> songNames = new ArrayList();
    private ArrayList<Node> brokerList= null;
    private File musicFilePath = new File("C:\\dataset1");
    private Socket socket;

    public Publisher(String ipAddress, int port, String startLetter, String finishLetter) {
        super(ipAddress, port);
        this.startLetter = startLetter;
        this.finishLetter = finishLetter;
    }

    public Publisher(Publisher temp) {
        super(temp.getIpAddress(), temp.getPort());
        this.brokerIp = temp.brokerIp;
        this.brokerPort = temp.brokerPort;
        this.startLetter = temp.startLetter;
        this.finishLetter = temp.finishLetter;
        this.artistNames = temp.artistNames;
        this.musicFiles = temp.musicFiles;
        this.songNames = temp.songNames;
        this.brokerList = temp.brokerList;
        this.musicFilePath = temp.musicFilePath;
        this.socket = temp.socket;
    }

    public String getBrokerIp() {
        return brokerIp;
    }

    public void setBrokerIp(String brokerIp) {
        this.brokerIp = brokerIp;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(int brokerPort) {
        this.brokerPort = brokerPort;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    private void startPublish() {
        readMusicFiles(musicFilePath);
        getBrokerList();
        push ();
    }

    public void startServer() {
        startPublish();
        ServerSocket listenerSocket = null;
        Socket connection=null;
        ArrayList<Thread> threads = new ArrayList();
        try {
            listenerSocket = new ServerSocket(port);
            while (true) {
                System.out.println("Server is up and waiting ");
                connection = listenerSocket.accept();
                Publisher temp = new Publisher(this);
                temp.setSocket(connection);
                Thread t = new Thread(temp);
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

    public void readMusicFiles(final File folder) {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                readMusicFiles(fileEntry);
            } else {
                if ( !fileEntry.getName().startsWith(".") && fileEntry.getName().endsWith(".mp3")) {
                    try {
                        Mp3File mp3 = new  Mp3File(fileEntry);
                        if (mp3.hasId3v2Tag()) {
                            ID3v2 id3v2Tag = mp3.getId3v2Tag();
                            String artistName = id3v2Tag.getArtist();
                            if (artistName != null && !artistName.equalsIgnoreCase("unknown") && !artistName.equalsIgnoreCase("null") && !artistName.equals("")) {
                                artistName = artistName.trim();
                                if (artistName.substring(0, 1).compareToIgnoreCase(startLetter) >= 0 && artistName.substring(0, 1).compareToIgnoreCase(finishLetter) <= 0) {
                                    if (!artistNames.contains(artistName)) {
                                        artistNames.add(artistName);
                                        musicFiles.add(new ArrayList());
                                        songNames.add(new ArrayList());
                                    }
                                    String nameFile = fileEntry.getName();
                                    int index = artistNames.indexOf(artistName);
                                    musicFiles.get(index).add(fileEntry);
                                    songNames.get(index).add(nameFile.substring(0, nameFile.length() - 4).trim());
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (UnsupportedTagException e) {
                        e.printStackTrace();
                    } catch (InvalidDataException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public void push ()  {
        for(Node broker : brokerList) {
            Socket socket = connect(broker.getIpAddress(),broker.getPort());
            ObjectOutputStream out = null;
            try {
                out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                System.out.println(in.readUTF());
                out.writeUTF("Push");
                out.flush();
                out.writeObject((Node) this);
                out.flush();
                out.writeObject(artistNames);
                out.flush();
                out.close();
                in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    disconnect(socket);
            }
        }
    }


    private void getBrokerList(){
        Socket socket = connect(brokerIp,brokerPort);
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            System.out.println(in.readUTF());
            out.writeUTF("BrokerList");
            out.flush();
            brokerList = (ArrayList<Node>)in.readObject();
            in.close();
            out.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            disconnect(socket);
        }
    }

    public void notifyFailure(Broker broker){
        Socket socket = connect(broker.getIpAddress(),broker.getPort());
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            System.out.println(in.readUTF());
            out.writeUTF("Broker Failed");
            out.flush();
            getBrokerList();
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() { publishListener(socket);
    }

    private void publishListener(Socket socket) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            out.writeUTF("Publisher: Connection Successful ");
            out.flush();
            String client = socket.getInetAddress().getHostAddress();
            System.out.println("Connected client : " + client);
            String request = in.readUTF();
            System.out.println("Request for: " + request + " from --> " + client);
            if (request.equalsIgnoreCase("Asking song")) {
                request = in.readUTF();  // mas stelnei to artist name
                boolean found = false;
                int position = artistNames.indexOf(request);
                if (position > -1) found = true;
                out.writeBoolean(found);
                out.flush();
                if (found) {
                    out.writeObject(songNames.get(position));
                    out.flush();
                    request = in.readUTF();
                    int temp = songNames.get(position).indexOf(request);
                    if (temp > -1){
                        out.writeBoolean(true);
                        out.flush();
                        ArrayList <Value> chunks = splitFile(musicFiles.get(position).get(temp), artistNames.get(position));
                        chunks.sort(null);
                        out.writeInt(chunks.size());
                        out.flush();
                        String ok = in.readUTF();
                        System.err.println(ok);
                        for (Value chunk : chunks){
                            out.writeObject(chunk);
                            out.flush();
                            ok = in.readUTF();//for synchronization
                        }
                        out.writeUTF("stop sending");
                        out.flush();
                    }
                    else {
                        out.writeBoolean(false);
                        out.flush();
                        out.writeObject(songNames.get(position));
                        out.flush();
                    }
                }
                else {System.err.println("Problem with artist list");}
            } else if (request.equals("List of Artists")) {
                out.writeObject(artistNames);
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<Value> splitFile(File file, String artistName) throws IOException {
        int counter = 1;
        ArrayList<Value> chunks = new ArrayList<>();
        int sizeOfChunk = 1024 * 512;
        byte[] buffer = new byte[sizeOfChunk];
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
            String name = file.getName();
            int temp = 0;
            while ((temp = in.read(buffer)) > 0) {
                Value chunk = new Value (name,buffer,temp, artistName,counter++);
                chunks.add(chunk);
            }
        }
        return chunks;
    }

    public static void main (String[] args) {
        Publisher p1 = new Publisher("192.168.2.2",6574,"A","M");
        //Publisher p1 = new Publisher("192.168.2.2",7254,"N","Z");
        p1.setBrokerIp("192.168.2.2");
        p1.setBrokerPort(4251);
        p1.startServer();
    }
}