package ru.yandex.practicum.commerce.store.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.ShoppingStoreClient;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.store.dto.ProductApiDto;
import ru.yandex.practicum.commerce.store.dto.ProductApiRequestDto;
import ru.yandex.practicum.commerce.store.dto.ProductPageDto;
import ru.yandex.practicum.commerce.store.entity.Product;
import ru.yandex.practicum.commerce.store.exception.ResourceNotFoundException;
import ru.yandex.practicum.commerce.store.repository.ProductRepository;
import ru.yandex.practicum.commerce.store.service.ProductService;

import java.util.Comparator;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor
public class ProductController implements ShoppingStoreClient {

    private final ProductService productService;
    private final ProductRepository productRepository;

    // === Feign contract (ShoppingStoreClient) ===
    @Override
    @GetMapping("/products")
    public List<ProductDto> getProducts(@RequestParam(required = false) ProductCategory category) {
        return productService.getProducts(category);
    }

    @Override
    @GetMapping("/products/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @Override
    @PostMapping("/admin/products")
    public ProductDto createProduct(@RequestBody ProductCreateUpdateDto dto) {
        return productService.createProduct(dto);
    }

    @Override
    @PutMapping("/admin/products/{id}")
    public ProductDto updateProduct(@PathVariable Long id, @RequestBody ProductCreateUpdateDto dto) {
        return productService.updateProduct(id, dto);
    }

    @Override
    @DeleteMapping("/admin/products/{id}")
    public void deleteProduct(@PathVariable Long id) {
        productService.deleteProduct(id);
    }

    @Override
    @PutMapping("/admin/products/{id}/availability")
    public ProductDto updateAvailability(@PathVariable Long id, @RequestParam ProductAvailability availability) {
        return productService.updateAvailability(id, availability);
    }

    // === API v1 endpoints ===
    @PutMapping("/api/v1/shopping-store")
    public ProductApiDto addOrUpdateProductApiV1(@RequestBody ProductApiRequestDto dto) {
        if (dto == null) {
            throw new IllegalArgumentException("Request body is required");
        }
        if (dto.getProductName() == null || dto.getProductName().isBlank()) {
            throw new IllegalArgumentException("productName is required");
        }
        ProductCreateUpdateDto createDto = ProductCreateUpdateDto.builder()
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
        if (dto.getQuantityState() != null) {
            productService.updateAvailability(result.getId(), dto.getQuantityState());
        }
        if (dto.getPrice() != null) {
            Product product = productRepository.findById(result.getId()).orElseThrow();
            product.setPrice(dto.getPrice());
            productRepository.save(product);
        }
        if (dto.getProductState() == ProductState.DEACTIVATE) {
            productService.deleteProduct(result.getId());
        }
        return toApiDto(productRepository.findById(result.getId()).orElseThrow());
    }

    @PostMapping("/api/v1/shopping-store")
    public ProductApiDto createProductApiV1(@RequestBody ProductApiRequestDto dto) {
        if (dto == null || dto.getProductName() == null || dto.getProductName().isBlank()) {
            throw new IllegalArgumentException("productName is required");
        }
        ProductCreateUpdateDto createDto = ProductCreateUpdateDto.builder()
                .name(dto.getProductName())
                .description(dto.getDescription())
                .photos(dto.getImageSrc() != null ? List.of(dto.getImageSrc()) : List.of())
                .category(dto.getProductCategory() != null ? dto.getProductCategory() : ProductCategory.CONTROL)
                .build();
        var result = productService.createProduct(createDto);
        if (dto.getQuantityState() != null) {
            productService.updateAvailability(result.getId(), dto.getQuantityState());
        }
        if (dto.getPrice() != null) {
            Product product = productRepository.findById(result.getId()).orElseThrow();
            product.setPrice(dto.getPrice());
            productRepository.save(product);
        }
        if (dto.getProductState() == ProductState.DEACTIVATE) {
            productService.deleteProduct(result.getId());
        }
        return toApiDto(productRepository.findById(result.getId()).orElseThrow());
    }

    @GetMapping("/api/v1/shopping-store")
    public ProductPageDto getProductsApiV1(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "150") int size,
            @RequestParam(required = false) String sort) {
        var products = category != null
                ? productRepository.findByStateAndCategory(ProductState.ACTIVE, category)
                : productRepository.findByState(ProductState.ACTIVE);
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

    @GetMapping("/api/v1/shopping-store/{id}")
    public ProductApiDto getProductApiV1(@PathVariable Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Product not found: " + id));
        return toApiDto(product);
    }

    @PostMapping("/api/v1/shopping-store/removeProductFromStore")
    public ProductApiDto removeProductApiV1(
            @RequestParam(required = false) Long productId,
            @RequestBody(required = false) Map<String, Object> body) {
        Long id = productId;
        if (id == null && body != null && body.containsKey("productId")) {
            Object val = body.get("productId");
            if (val instanceof Number) {
                id = ((Number) val).longValue();
            } else if (val != null) {
                try {
                    id = Long.parseLong(val.toString());
                } catch (NumberFormatException ignored) {}
            }
        }
        if (id == null) {
            throw new IllegalArgumentException("productId is required");
        }
        productService.deleteProduct(id);
        return toApiDto(productRepository.findById(id).orElseThrow());
    }

    @PostMapping("/api/v1/shopping-store/quantityState")
    public ProductApiDto setQuantityStateApiV1(
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) ProductAvailability quantityState,
            @RequestBody(required = false) Map<String, Object> body) {
        Long id = productId;
        if (id == null && body != null && body.containsKey("productId")) {
            Object val = body.get("productId");
            if (val instanceof Number) {
                id = ((Number) val).longValue();
            } else if (val != null) {
                try {
                    id = Long.parseLong(val.toString());
                } catch (NumberFormatException ignored) {}
            }
        }
        if (id == null) {
            throw new IllegalArgumentException("productId is required");
        }
        if (quantityState == null && body != null && body.containsKey("quantityState")) {
            Object val = body.get("quantityState");
            if (val != null) {
                try {
                    quantityState = ProductAvailability.valueOf(val.toString());
                } catch (IllegalArgumentException ignored) {}
            }
        }
        if (quantityState != null) {
            productService.updateAvailability(id, quantityState);
        }
        return toApiDto(productRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("Product not found: " + id)));
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
