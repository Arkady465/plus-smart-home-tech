package ru.yandex.practicum.commerce.cart;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableFeignClients(basePackages = "ru.yandex.practicum.commerce.api")
@ComponentScan(basePackages = {"ru.yandex.practicum.commerce.cart", "ru.yandex.practicum.commerce.api"})
public class ShoppingCartApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShoppingCartApplication.class, args);
    }
}
