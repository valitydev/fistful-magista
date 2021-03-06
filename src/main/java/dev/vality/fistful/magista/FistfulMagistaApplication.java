package dev.vality.fistful.magista;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication(scanBasePackages = {"dev.vality.fistful.magista"})
public class FistfulMagistaApplication {

    public static void main(String... args) {
        SpringApplication.run(FistfulMagistaApplication.class, args);
    }
}
