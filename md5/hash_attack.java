import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.io.*;
import java.util.*;

public class hash_attack {

    public static void main(String[] args){
        
        String word = "r41nb0w";
        String hash = getHash(word);
        String pwd = findHash(hash);
        System.out.println(hash); 
        System.out.println(pwd); 
    
    }
    
    private static String findHash(String hash) {
	
		String pwd = "";
		boolean found = false;
		try {
			FileInputStream fstream = new FileInputStream("./dictionary/lowercase.rand");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while (((line = br.readLine()) != null) && (found == false)) {
				if (getHash(line).equals(hash)) {
					System.out.println("FOUND!!!");
					found = true;
					pwd = line;
				}
			}
			
			in.close();
		} catch (Exception e) {
			/* just print the error stack and exit. */
			e.printStackTrace();
			System.exit(1);
		}
		if (found == false) {
			pwd = "NOT FOUND";
		}
		
		return pwd;
	}
	
    public static String getHash(String word) {

        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }
}
