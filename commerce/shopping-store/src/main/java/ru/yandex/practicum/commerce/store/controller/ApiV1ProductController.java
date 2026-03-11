package ru.yandex.practicum.commerce.store.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.ProductAvailability;
import ru.yandex.practicum.commerce.dto.ProductCategory;
import ru.yandex.practicum.commerce.store.dto.ProductApiDto;
import ru.yandex.practicum.commerce.store.dto.ProductApiRequestDto;
import ru.yandex.practicum.commerce.store.dto.ProductPageDto;
import ru.yandex.practicum.commerce.store.entity.Product;
import ru.yandex.practicum.commerce.store.repository.ProductRepository;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.store.service.ProductService;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class ApiV1ProductController {

    private final ProductService productService;
    private final ProductRepository productRepository;

    @PutMapping
    public ProductApiDto addOrUpdateProduct(@RequestBody ProductApiRequestDto dto) {
        if (dto.getProductName() == null || dto.getProductName().isBlank()) {
            throw new IllegalArgumentException("productName is required");
        }
        ru.yandex.practicum.commerce.dto.ProductCreateUpdateDto createDto =
                ru.yandex.practicum.commerce.dto.ProductCreateUpdateDto.builder()
                        .name(dto.getProductName())
                        .description(dto.getDescription())
                        .photos(dto.getImageSrc() != null ? List.of(dto.getImageSrc()) : List.of())
                        .category(dto.getProductCategory() != null ? dto.getProductCategory() : ProductCategory.CONTROL)
                        .build();
        ProductDto result;
        var existing = productRepository.findAll().stream()
                .filter(p -> p.getName().equals(dto.getProductName()))
                .findFirst();
        if (existing.isPresent()) {
            result = productService.updateProduct(existing.get().getId(), createDto);
        } else {
            result = productService.createProduct(createDto);
        }
        Product product = productRepository.findById(result.getId()).orElseThrow();
        if (dto.getQuantityState() != null) {
            productService.updateAvailability(result.getId(), dto.getQuantityState());
        }
        if (dto.getPrice() != null) {
            product.setPrice(dto.getPrice());
            productRepository.save(product);
        }
        return toApiDto(productRepository.findById(result.getId()).orElseThrow());
    }

    @PostMapping
    public ProductApiDto createProduct(@RequestBody ProductApiRequestDto dto) {
        ru.yandex.practicum.commerce.dto.ProductCreateUpdateDto createDto =
                ru.yandex.practicum.commerce.dto.ProductCreateUpdateDto.builder()
                        .name(dto.getProductName())
                        .description(dto.getDescription())
                        .photos(dto.getImageSrc() != null ? List.of(dto.getImageSrc()) : List.of())
                        .category(dto.getProductCategory() != null ? dto.getProductCategory() : ProductCategory.CONTROL)
                        .build();
        var result = productService.createProduct(createDto);
        if (dto.getQuantityState() != null) {
            productService.updateAvailability(result.getId(), dto.getQuantityState());
        }
        Product product = productRepository.findById(result.getId()).orElseThrow();
        if (dto.getPrice() != null) {
            product.setPrice(dto.getPrice());
            productRepository.save(product);
        }
        return toApiDto(productRepository.findById(result.getId()).orElseThrow());
    }

    @GetMapping
    public ProductPageDto getProducts(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "150") int size,
            @RequestParam(required = false) String sort) {
        var products = category != null
                ? productRepository.findByStateAndCategory(ru.yandex.practicum.commerce.dto.ProductState.ACTIVE, category)
                : productRepository.findByState(ru.yandex.practicum.commerce.dto.ProductState.ACTIVE);
        if (sort != null && sort.contains(",")) {
            var parts = sort.split(",");
            var desc = parts.length > 1 && "DESC".equalsIgnoreCase(parts[1].trim());
            var cmp = Comparator.<Product, String>comparing(Product::getName);
            products = products.stream()
                    .sorted(desc ? cmp.reversed() : cmp)
                    .collect(Collectors.toList());
        }
        int from = page * size;
        int to = Math.min(from + size, products.size());
        var paged = products.subList(Math.min(from, products.size()), to);
        var content = paged.stream().map(this::toApiDto).collect(Collectors.toList());
        return ProductPageDto.builder().content(content).build();
    }

    @GetMapping("/{id}")
    public ProductApiDto getProduct(@PathVariable Long id) {
        return toApiDto(productRepository.findById(id).orElseThrow(() -> new RuntimeException("Product not found: " + id)));
    }

    @PostMapping("/removeProductFromStore")
    public void removeProduct(@RequestParam(required = false) Long productId) {
        if (productId != null) productService.deleteProduct(productId);
    }

    @PostMapping("/quantityState")
    public ProductApiDto setQuantityState(
            @RequestParam Long productId,
            @RequestParam ProductAvailability quantityState) {
        productService.updateAvailability(productId, quantityState);
        return toApiDto(productRepository.findById(productId).orElseThrow());
    }

    private ProductApiDto toApiDto(Product product) {
        var availability = product.getQuantity() <= 0 ? ProductAvailability.ENDED
                : product.getQuantity() < 10 ? ProductAvailability.FEW
                : product.getQuantity() <= 100 ? ProductAvailability.ENOUGH : ProductAvailability.MANY;
        return ProductApiDto.builder()
                .id(product.getId())
                .productName(product.getName())
                .description(product.getDescription())
                .imageSrc(product.getPhotos() != null && !product.getPhotos().isEmpty() ? product.getPhotos().get(0) : null)
                .quantityState(availability)
                .productState(product.getState())
                .productCategory(product.getCategory())
                .price(product.getPrice())
                .build();
    }

}
