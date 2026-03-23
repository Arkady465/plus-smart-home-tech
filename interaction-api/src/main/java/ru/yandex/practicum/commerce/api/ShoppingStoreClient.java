package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.ProductAvailability;
import ru.yandex.practicum.commerce.dto.ProductCategory;
import ru.yandex.practicum.commerce.dto.ProductCreateUpdateDto;
import ru.yandex.practicum.commerce.dto.ProductDto;

import java.util.List;

@FeignClient(name = "shopping-store")
public interface ShoppingStoreClient {

    @GetMapping("/products")
    List<ProductDto> getProducts(@RequestParam(required = false) ProductCategory category);

    @GetMapping("/products/{id}")
    ProductDto getProduct(@PathVariable Long id);

    @PostMapping("/admin/products")
    ProductDto createProduct(@RequestBody ProductCreateUpdateDto dto);

    @PutMapping("/admin/products/{id}")
    ProductDto updateProduct(@PathVariable Long id, @RequestBody ProductCreateUpdateDto dto);

    @DeleteMapping("/admin/products/{id}")
    void deleteProduct(@PathVariable Long id);

    @PutMapping("/admin/products/{id}/availability")
    ProductDto updateAvailability(@PathVariable Long id, @RequestParam ProductAvailability availability);
}
