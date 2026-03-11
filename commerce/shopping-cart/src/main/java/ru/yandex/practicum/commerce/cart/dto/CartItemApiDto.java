package ru.yandex.practicum.commerce.cart.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CartItemApiDto {
    private Object productId;  // Long or String (UUID) for Postman compatibility
    private int quantity;
}
