package ru.yandex.practicum.commerce.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WarehouseProductDto {
    private Long id;
    private String productId;
    private int quantity;
    private Double width;
    private Double height;
    private Double depth;
    private Double weight;
    private Boolean fragile;
}
