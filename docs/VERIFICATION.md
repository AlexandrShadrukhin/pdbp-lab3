# Выполненные проверки

Дата: 9 ноября 2026 года. Windows, Temurin JDK 17.0.20.1, Gradle 8.14.4.

- Gradle Wrapper создан штатной задачей wrapper.
- После исправления ошибочного ожидания bigint вместо long в тесте выполнена полная команда clean build run: BUILD SUCCESSFUL.
- После commit/push и отдельного git clone команда clean build run выполнена в клоне повторно: BUILD SUCCESSFUL.
- JUnit XML в клоне: tests=27, failures=0, errors=0, skipped=0. Параметризованные случаи включены в число 27.
- Реальное чтение через format("pdbpline"), Spark SQL и план BatchScan проверены тестами.
- DemoApp: VERIFIED: count=12, total=780.0, amount>60=6.
- Private-репозиторий создан в подтверждённом аккаунте AlexandrShadrukhin; main отправлен через git push.
- IntelliJ IDEA установлена и запущена. Попытки открыть клон через launcher и File/Open не позволили подтвердить открытие проекта: управление диалогом выбора папки не сработало. Импорт, SDK в UI и запуск внутри IDEA не подтверждены. Есть переносимая конфигурация Gradle DemoApp.
- Docker не использовался: local Spark успешно работает без контейнеров.

Логи успешных запусков сохранены рядом с исходным проектом в outputs/verification-clean.log и outputs/verification-clone.log. Отчёт JUnit генерируется в build/reports/tests/test/index.html.
