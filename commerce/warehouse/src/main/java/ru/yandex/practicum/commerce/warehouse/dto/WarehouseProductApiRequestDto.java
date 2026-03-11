package ru.yandex.practicum.commerce.warehouse.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WarehouseProductApiRequestDto {
    private DimensionDto dimension;
    private String productId;
    private String weight;
    private String fragile;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DimensionDto {
        private String width;
        private String height;
        private String depth;
    }
}
