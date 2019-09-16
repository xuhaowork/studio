//package com.self.core.learningGraphx;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//
//public class GetClassFromJar {
//    public void test3Columns()
//            throws IOException
//    {
//        InputStream is = getClass().getResourceAsStream("/3Columns.csv");
//        InputStreamReader isr = new InputStreamReader(is);
//        BufferedReader br = new BufferedReader(isr);
//        String line;
//        while ((line = br.readLine()) != null)
//        {
//            CSVLineTokenizer tok = new CSVLineTokenizer(line);
//            assertEquals("Should be three columns in each row",3,tok.countTokens());
//        }
//        br.close();
//        isr.close();
//        is.close();
//    }
//
//
//}
