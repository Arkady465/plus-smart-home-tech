package ru.yandex.practicum.commerce.warehouse.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.commerce.dto.AddressDto;

import java.security.SecureRandom;

@Configuration
public class WarehouseConfig {

    private static final String[] ADDRESSES = new String[]{"ADDRESS_1", "ADDRESS_2"};

    @Bean
    public AddressDto currentAddress() {
        String addressValue = ADDRESSES[new SecureRandom().nextInt(ADDRESSES.length)];
        return AddressDto.builder()
                .country(addressValue)
                .city(addressValue)
                .street(addressValue)
                .house(addressValue)
                .apartment(addressValue)
                .flat(addressValue)
                .build();
    }
}
