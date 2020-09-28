package com.example.opalakia;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash_with_MD5 {

    public static BigInteger Hash (String ArtistName) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        md.update(ArtistName.getBytes());
        byte[] digest = md.digest();
        BigInteger no = new BigInteger(1, digest);
        return no;
    }

    public static BigInteger Hash (String ip, int port) {
        String temp = ip + Integer.toString(port);
        return Hash(temp);
    }
}