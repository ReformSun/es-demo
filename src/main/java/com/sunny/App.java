package com.sunny;

import com.sunny.client.ClientUtils;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class App {
    public static void main(String[] args) {
        System.out.println("Hello World");
    }

    public static void testMethod1(){
        try (RestHighLevelClient client = ClientUtils.initESClient();) {

        }catch (IOException e) {
            e.printStackTrace();
        }

    }
}
