package ru.yandex.practicum.commerce.cart.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.ShoppingCartClient;
import ru.yandex.practicum.commerce.cart.dto.CartItemApiDto;
import ru.yandex.practicum.commerce.cart.service.CartService;
import ru.yandex.practicum.commerce.dto.CartDto;
import ru.yandex.practicum.commerce.dto.CartItemDto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class CartController implements ShoppingCartClient {

    private final CartService cartService;

    // === Feign contract (ShoppingCartClient) ===
    @Override
    @GetMapping("/carts/{username}")
    public CartDto getCart(@PathVariable String username) {
        return cartService.getCart(username);
    }

    @Override
    @PostMapping("/carts/{username}/items")
    public CartDto addItem(@PathVariable String username, @RequestBody CartItemDto item) {
        return cartService.addItem(username, item);
    }

    @Override
    @PutMapping("/carts/{username}/items/{productId}")
    public CartDto updateItemQuantity(@PathVariable String username, @PathVariable String productId,
                                      @RequestParam int quantity) {
        return cartService.updateItemQuantity(username, productId, quantity);
    }

    @Override
    @DeleteMapping("/carts/{username}/items/{productId}")
    public CartDto removeItem(@PathVariable String username, @PathVariable String productId) {
        return cartService.removeItem(username, productId);
    }

    @Override
    @PostMapping("/carts/{username}/deactivate")
    public CartDto deactivateCart(@PathVariable String username) {
        return cartService.deactivateCart(username);
    }

    // === API v1 endpoints ===
    @GetMapping("/api/v1/shopping-cart")
    public Map<String, Object> getCartApiV1(@RequestParam String username) {
        return toApiV1(cartService.getCart(username));
    }

    @PutMapping("/api/v1/shopping-cart")
    public Map<String, Object> updateCartApiV1(@RequestParam String username, @RequestBody(required = false) Map<String, Object> body) {
        if (body != null && !body.isEmpty()) {
            if (body.containsKey("items") && body.get("items") instanceof List) {
                for (Object o : (List<?>) body.get("items")) {
                    if (o instanceof Map) {
                        Map<?, ?> item = (Map<?, ?>) o;
                        Object pid = item.get("productId");
                        Object qty = item.get("quantity");
                        if (pid != null) {
                            int quantity = qty instanceof Number ? ((Number) qty).intValue() : 0;
                            cartService.addItem(username, CartItemDto.builder()
                                    .productId(pid)
                                    .quantity(quantity)
                                    .build());
                        }
                    }
                }
            } else {
                cartService.updateCartFromMap(username, body);
            }
        }
        return toApiV1(cartService.getCart(username));
    }

    @PostMapping("/api/v1/shopping-cart/change-quantity")
    public Map<String, Object> changeQuantityApiV1(@RequestParam String username, @RequestBody CartItemApiDto item) {
        String pid = toProductIdString(item.getProductId());
        if (pid == null) return toApiV1(cartService.getCart(username));
        int qty = item.getQuantity();
        if (item.getNewQuantity() != null) {
            qty = item.getNewQuantity();
        }
        return toApiV1(cartService.updateItemQuantity(username, pid, qty));
    }

    @PostMapping("/api/v1/shopping-cart/remove")
    public Map<String, Object> removeItemApiV1(@RequestParam String username, @RequestBody Object body) {
        if (body instanceof List) {
            List<String> ids = ((List<?>) body).stream()
                    .filter(o -> o != null)
                    .map(Object::toString)
                    .toList();
            return toApiV1(cartService.removeItems(username, ids));
        }
        if (body instanceof Map) {
            Object pid = ((Map<?, ?>) body).get("productId");
            if (pid != null) {
                return toApiV1(cartService.removeItem(username, pid.toString()));
            }
        }
        return toApiV1(cartService.getCart(username));
    }

    @DeleteMapping("/api/v1/shopping-cart")
    public Map<String, Object> deactivateCartApiV1(@RequestParam String username) {
        return toApiV1(cartService.deactivateCart(username));
    }

    private static String toProductIdString(Object o) {
        if (o == null) return null;
        return o.toString();
    }

    private static Map<String, Object> toApiV1(CartDto dto) {
        Map<String, Integer> products = new HashMap<>();
        if (dto.getItems() != null) {
            for (CartItemDto item : dto.getItems()) {
                if (item != null && item.getProductId() != null) {
                    products.put(item.getProductId().toString(), item.getQuantity());
                }
            }
        }
        Map<String, Object> response = new HashMap<>();
        response.put("id", dto.getId());
        response.put("username", dto.getUsername());
        response.put("state", dto.getState());
        response.put("items", dto.getItems());
        response.put("products", products);
        return response;
    }
}
