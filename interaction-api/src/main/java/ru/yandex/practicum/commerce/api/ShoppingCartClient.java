package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.CartDto;
import ru.yandex.practicum.commerce.dto.CartItemDto;

@FeignClient(name = "shopping-cart")
public interface ShoppingCartClient {

    @GetMapping("/carts/{username}")
    CartDto getCart(@PathVariable String username);

    @PostMapping("/carts/{username}/items")
    CartDto addItem(@PathVariable String username, @RequestBody CartItemDto item);

    @PutMapping("/carts/{username}/items/{productId}")
    CartDto updateItemQuantity(@PathVariable String username, @PathVariable String productId,
                               @RequestParam int quantity);

    @DeleteMapping("/carts/{username}/items/{productId}")
    CartDto removeItem(@PathVariable String username, @PathVariable String productId);

    @PostMapping("/carts/{username}/deactivate")
    CartDto deactivateCart(@PathVariable String username);
}
