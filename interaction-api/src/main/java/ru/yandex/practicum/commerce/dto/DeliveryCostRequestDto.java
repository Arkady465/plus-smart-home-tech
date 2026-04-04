package ru.yandex.practicum.commerce.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeliveryCostRequestDto {
    private AddressDto warehouseAddress;
    private AddressDto deliveryAddress;
    private Double weight;
    private Double volume;
    private boolean fragile;
}

