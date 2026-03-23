package ru.yandex.practicum.commerce.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AvailabilityCheckResponseDto {
    /**
     * true если всех товаров достаточно.
     */
    private boolean available;
    /**
     * ID товаров, которых не хватает на складе.
     */
    private List<Object> insufficientProductIds;
}
