package com.hikvision;

import java.util.stream.Stream;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//        System.out.println( "Hello World!" );
        Stream.of(1,2,3).forEach(System.out::println);
    }
}
