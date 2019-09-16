package org.apache.spark.binary.load;

import org.apache.commons.codec.binary.Hex;
public  final class Conversion {
    public static String bytes2hex(byte[] bytes)
    {
        return ( new String(Hex.encodeHex(bytes)));
    }

}
