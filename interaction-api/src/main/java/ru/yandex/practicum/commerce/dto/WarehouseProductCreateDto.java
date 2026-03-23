package ru.yandex.practicum.commerce.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WarehouseProductCreateDto {
    @NotNull
    private String productId;
    @PositiveOrZero
    private int quantity;
    private Double width;
    private Double height;
    private Double depth;
    private Double weight;
    private Boolean fragile;
}
