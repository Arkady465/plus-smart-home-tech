package ru.yandex.practicum.commerce.warehouse.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.warehouse.model.Stock;
import ru.yandex.practicum.commerce.warehouse.repository.StockRepository;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping
@RequiredArgsConstructor
public class StockController {

    private final StockRepository stockRepository;

    @GetMapping("/{productId}")
    public ResponseEntity<Stock> getStock(@PathVariable Long productId) {
        return stockRepository.findByProductId(productId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{productId}/reserve")
    public ResponseEntity<Stock> reserve(@PathVariable Long productId, @RequestBody Map<String, Integer> body) {
        int quantity = body.getOrDefault("quantity", 0);
        return stockRepository.findByProductId(productId)
                .filter(s -> s.getQuantity() >= quantity)
                .map(s -> {
                    s.setQuantity(s.getQuantity() - quantity);
                    return ResponseEntity.ok(stockRepository.save(s));
                })
                .orElse(ResponseEntity.badRequest().build());
    }

    @PostMapping
    public Stock create(@RequestBody Stock stock) {
        return stockRepository.save(stock);
    }

    @GetMapping
    public List<Stock> getAll() {
        return stockRepository.findAll();
    }
}
