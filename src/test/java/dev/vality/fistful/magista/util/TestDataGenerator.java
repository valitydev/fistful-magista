package dev.vality.fistful.magista.util;

import lombok.experimental.UtilityClass;
import org.instancio.Instancio;
import org.instancio.Model;
import org.instancio.Select;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@UtilityClass
public class TestDataGenerator {

    public static <T> T create(Class<T> clazz) {
        Model<T> model = Instancio.of(clazz)
                .supply(Select.all(LocalDateTime.class), context -> {
                    // Генерируем значение с миллисекундной точностью
                    return LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
                })
                .toModel();

        return Instancio.create(model);
    }
}