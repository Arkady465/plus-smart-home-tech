package ru.yandex.practicum.commerce.cart.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
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
    @JsonAlias("newQuantity")
    private Integer newQuantity;  // alternative to quantity for change-quantity endpoint
}
