package structuredStreaming.sink.util;

import com.sun.crypto.provider.SunJCE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.security.Security;

/**
 * @Author: junping.chi@luckincoffee.com
 * @Date: 2019/7/27 13:43
 * @Description:
 */

public class DatabaseEncryption {
    private static Logger logger = LoggerFactory.getLogger(DatabaseEncryption.class);
    private static String strDefaultKey = "goodluck";
    private static Cipher encryptCipher = null;
    private static Cipher decryptCipher = null;

    public DatabaseEncryption() {
    }

    public static String encrypt(String strMing) {
        try {
            return byteArr2HexStr(encrypt(strMing.getBytes("UTF8")));
        } catch (Exception var2) {
            logger.error("something goes wrong when encrypt", var2);
            throw new RuntimeException(var2);
        }
    }

    public static String decrypt(String strMi) {
        try {
            return new String(decrypt(hexStr2ByteArr(strMi)), "UTF8");
        } catch (Exception var2) {
            logger.error("something goes wrong when decrypt", var2);
            throw new RuntimeException(var2);
        }
    }

    private static String byteArr2HexStr(byte[] arrB) throws Exception {
        int iLen = arrB.length;
        StringBuffer sb = new StringBuffer(iLen * 2);

        for (int i = 0; i < iLen; ++i) {
            int intTmp;
            for (intTmp = arrB[i]; intTmp < 0; intTmp += 256) {
            }

            if (intTmp < 16) {
                sb.append("0");
            }

            sb.append(Integer.toString(intTmp, 16));
        }

        return sb.toString();
    }

    private static byte[] hexStr2ByteArr(String strIn) throws Exception {
        byte[] arrB = strIn.getBytes("UTF8");
        int iLen = arrB.length;
        byte[] arrOut = new byte[iLen / 2];

        for (int i = 0; i < iLen; i += 2) {
            String strTmp = new String(arrB, i, 2, "UTF8");
            arrOut[i / 2] = (byte) Integer.parseInt(strTmp, 16);
        }

        return arrOut;
    }

    private static byte[] encrypt(byte[] arrB) throws Exception {
        return encryptCipher.doFinal(arrB);
    }

    private static byte[] decrypt(byte[] arrB) throws Exception {
        return decryptCipher.doFinal(arrB);
    }

    private static Key getKey(byte[] arrBTmp) throws Exception {
        byte[] arrB = new byte[8];

        for (int i = 0; i < arrBTmp.length && i < arrB.length; ++i) {
            arrB[i] = arrBTmp[i];
        }

        Key key = new SecretKeySpec(arrB, "DES");
        return key;
    }

    public static void main(String[] args) throws Exception {
        String str1 = "1qaz2wsx";
        String str2 = encrypt(str1);
        String deStr = decrypt("61de282fbe93652a704260d818d1b91d");
        System.out.println("密文:" + str2);
        System.out.println("明文:" + deStr);
    }

    static {
        Security.addProvider(new SunJCE());

        try {
            Key key = getKey(strDefaultKey.getBytes("UTF8"));
            encryptCipher = Cipher.getInstance("DES");
            encryptCipher.init(1, key);
            decryptCipher = Cipher.getInstance("DES");
            decryptCipher.init(2, key);
        } catch (Exception var1) {
            logger.error("something goes wrong when init Cipher", var1);
        }

    }
}
