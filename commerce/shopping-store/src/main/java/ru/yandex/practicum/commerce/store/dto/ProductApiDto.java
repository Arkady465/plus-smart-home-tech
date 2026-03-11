package ru.yandex.practicum.commerce.store.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.dto.ProductAvailability;
import ru.yandex.practicum.commerce.dto.ProductCategory;
import ru.yandex.practicum.commerce.dto.ProductState;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductApiDto {
    private Long id;
    private String productName;
    private String description;
    private String imageSrc;
    private ProductAvailability quantityState;
    private ProductState productState;
    private ProductCategory productCategory;
    private Double price;
}
