package ru.yandex.practicum.commerce.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeliveryDto {
    private Long id;
    private Long orderId;

    private AddressDto from;
    private AddressDto to;

    private Double weight;
    private Double volume;
    private boolean fragile;

    private DeliveryStatus status;
    private Double deliveryCost;
}

