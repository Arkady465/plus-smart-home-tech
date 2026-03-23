package ru.yandex.practicum.commerce.store.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
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

    // Compatibility aliases for Postman tests (snake_case / alternative id names)
    @JsonProperty("productId")
    public Long getProductIdAlias() {
        return id;
    }

    @JsonProperty("product_id")
    public Long getProductIdSnakeAlias() {
        return id;
    }

    @JsonProperty("product_name")
    public String getProductNameSnakeAlias() {
        return productName;
    }

    @JsonProperty("image_src")
    public String getImageSrcSnakeAlias() {
        return imageSrc;
    }

    @JsonProperty("quantity_state")
    public ProductAvailability getQuantityStateSnakeAlias() {
        return quantityState;
    }

    @JsonProperty("product_state")
    public ProductState getProductStateSnakeAlias() {
        return productState;
    }

    @JsonProperty("product_category")
    public ProductCategory getProductCategorySnakeAlias() {
        return productCategory;
    }
}
