package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.*;

/**
 * Fallback при недоступности сервиса warehouse.
 */
@Component
public class WarehouseClientFallback implements FallbackFactory<WarehouseClient> {

    @Override
    public WarehouseClient create(Throwable cause) {
        return new WarehouseClient() {
            @Override
            public java.util.List<WarehouseProductDto> getAllProducts() {
                throw new WarehouseUnavailableException("Warehouse service is temporarily unavailable", cause);
            }

            @Override
            public WarehouseProductDto addProduct(WarehouseProductCreateDto dto) {
                throw new WarehouseUnavailableException("Warehouse service is temporarily unavailable", cause);
            }

            @Override
            public WarehouseProductDto replenish(Long productId, int quantity) {
                throw new WarehouseUnavailableException("Warehouse service is temporarily unavailable", cause);
            }

            @Override
            public AvailabilityCheckResponseDto checkAvailability(AvailabilityCheckRequestDto request) {
                throw new WarehouseUnavailableException("Warehouse service is temporarily unavailable. Please try again later.", cause);
            }

            @Override
            public AddressDto getAddress() {
                throw new WarehouseUnavailableException("Warehouse service is temporarily unavailable", cause);
            }
        };
    }

    public static class WarehouseUnavailableException extends RuntimeException {
        public WarehouseUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
