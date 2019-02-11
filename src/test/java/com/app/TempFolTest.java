package com.app;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TempFolTest {



    @Test
    public void test() throws IOException {

        //C:\Users\IGOR_D~1\AppData\Local\Temp\junit8942427978229981064

        String path = "C:\\Users\\IGOR_D~1\\AppData\\Local\\Temp\\junit8942427978229981064";

        List<String> list = new ArrayList<>();

        Files.newDirectoryStream(Paths.get(path),
                p -> p.toString().endsWith(".csv"))
                .forEach(System.out::println);

        list.forEach( System.out::println );
    }

}
