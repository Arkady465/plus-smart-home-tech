package ru.yandex.practicum.commerce.cart.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.cart.dto.CartApiRequestDto;
import ru.yandex.practicum.commerce.cart.dto.CartItemApiDto;
import ru.yandex.practicum.commerce.cart.service.CartService;
import ru.yandex.practicum.commerce.dto.CartDto;
import ru.yandex.practicum.commerce.dto.CartItemDto;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
public class ApiV1CartController {

    private final CartService cartService;

    @GetMapping
    public CartDto getCart(@RequestParam String username) {
        return cartService.getCart(username);
    }

    @PutMapping
    public CartDto updateCart(@RequestParam String username, @RequestBody(required = false) CartApiRequestDto body) {
        if (body != null && body.getItems() != null) {
            for (CartItemApiDto item : body.getItems()) {
                Long pid = toLong(item.getProductId());
                if (pid != null) {
                    cartService.addItem(username, CartItemDto.builder()
                            .productId(pid)
                            .quantity(item.getQuantity())
                            .build());
                }
            }
        }
        return cartService.getCart(username);
    }

    @PostMapping("/change-quantity")
    public CartDto changeQuantity(@RequestParam String username, @RequestBody CartItemApiDto item) {
        Long pid = toLong(item.getProductId());
        if (pid == null) return cartService.getCart(username);
        return cartService.updateItemQuantity(username, pid, item.getQuantity());
    }

    @PostMapping("/remove")
    public CartDto removeItem(@RequestParam String username, @RequestBody CartItemApiDto item) {
        Long pid = toLong(item.getProductId());
        if (pid == null) return cartService.getCart(username);
        return cartService.removeItem(username, pid);
    }

    private static Long toLong(Object o) {
        if (o == null) return null;
        if (o instanceof Number) return ((Number) o).longValue();
        try {
            return Long.parseLong(o.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @DeleteMapping
    public CartDto deactivateCart(@RequestParam String username) {
        return cartService.deactivateCart(username);
    }
}
