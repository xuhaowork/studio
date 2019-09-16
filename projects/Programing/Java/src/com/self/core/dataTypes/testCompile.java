package com.self.core.dataTypes;

import com.self.base.BaseMain;

/**
 * java的基本数据类型
 * ----
 * 1.基本数据类型
 * --1.1 数值类型
 * ----1.1.1 整数 byte short int long --分别是: 1, 2, 4, 8字节
 * ----1.1.2 浮点型 float double --分别是: 4, 8字节  [符号位 乘积 指数]
 *
 * --1.2 字符型 char --2字节 java采用unicode16位定长编码
 * --1.3 布尔型 boolean
 *
 * 2.引用类型
 * --2.1 类
 * --2.2 接口 interface
 * --2.3 数组 array
 */
public class testCompile extends BaseMain {
    public static void main(String[] args) {
        System.out.println("this is a test file.");

        /* 整数类型 */
        byte b1= 123;
        int i1 = 129;
        long l1 = 122354544633354L;
        double d1 = 21343433232.0001545D;

        int b11 = b1;

        println(b11);

        /* 2进制/8进制/16进制数 --binary/octal/hex */
        int binary_1 = 0b11111111;
        int eight_1 = 0377;
        int hex_1 = 0xff;

        println(String.format("binary, %d", binary_1));
        println(String.format("eight, %d", eight_1));
        println(String.format("ten, %d", hex_1));


        /* 小数类型 都是拆分为: 符号位 乘积 指数 三个部分。分为单精度浮点数和双精度浮点数 */


        /*
        注意一些精度转换引起的损失:
        1)本身会损失;
        2)大小计算数值可能越界时最好不要用a - b > 0; 因为它和 a > b不等价;
        */
        long x1 = 12345678912346L;
        long x2 = 12345678912345L;
        boolean equal2 = (float) x2 == (float) x1;

        System.out.println(x1 == x2);
        System.out.println(equal2);

        int int1 = 1 << 31;
        // 1073741824
        //-2147483647
        System.out.println(int1 > 1);
        System.out.println(int1 - 1 > 0);


        /* char类型 */
        char a = 'A';
        println(a);

        char b = 65;
        println(b);

        char c = '\u0041';
        println(c);



    }

}
