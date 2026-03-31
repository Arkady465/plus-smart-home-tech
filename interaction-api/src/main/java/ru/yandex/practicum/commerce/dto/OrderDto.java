package ru.yandex.practicum.commerce.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto {
    private Long id;
    private String username;
    private OrderStatus status;

    private Long cartId;
    private Long paymentId;
    private Long deliveryId;

    private List<OrderItemDto> items;

    private Double totalCost;
    private Double productsCost;
    private Double deliveryCost;

    private Double weight;
    private Double volume;
    private boolean fragile;
}

