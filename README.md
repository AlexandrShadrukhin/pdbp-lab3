# pdbp-lab3

Лабораторная №3 по дисциплине «Параллельная и распределённая обработка баз данных».
Собственный построчный формат и настоящий batch-коннектор Apache Spark DataSource V2.
Java 17, Spark 3.5.8 (Scala 2.12), Gradle Wrapper 8.14.4, JUnit 5.

## Формат PDBP/1

Файл UTF-8 без BOM. Первая строка — сигнатура, вторая — схема, затем одна физическая строка на запись:

~~~text
PDBP/1
id:LONG|name:STRING|amount:DOUBLE
1|Иван|10.0
2|Анна|20.5
3|Текст\|разделитель|30.0
~~~

Реальный файл: [examples/sample.pdbp](examples/sample.pdbp).
Имена колонок: [A-Za-z_][A-Za-z0-9_]*, уникальные с учётом регистра. Схема непустая;
число колонок произвольное, демонстрационная схема содержит id, name, amount.
Типы строго LONG, STRING, DOUBLE. NULL не поддерживается; пустая строка допустима только для STRING.
LONG — десятичное целое со знаком минус по необходимости, диапазон Java Long.
DOUBLE — конечное десятичное число, допускаются дробная часть и экспонента e/E со знаком;
NaN, Infinity, переполнение, пробелы, шестнадцатеричные числа запрещены. Используется точность IEEE 754 double.

Разделитель — |. Экранирование: \\ для обратного слеша, \| для разделителя,
\n для LF, \r для CR, \t для табуляции. Неизвестное экранирование и незавершённый слеш — ошибка.
Переводы строк внутри STRING записываются как два символа \n или \r; необработанная табуляция запрещена.
Чтение принимает LF/CRLF (BufferedReader также распознаёт CR); writer всегда пишет LF.
Последний перевод строки необязателен. Пустая строка данных не пропускается, а разбирается по схеме.
Некорректный UTF-8 отклоняется. Все значения имеют строго заданные типы; writer принимает Long, String, Double.
Метаданные в версии 1 отсутствуют. Пустой набор — только две строки заголовка.

## Архитектура

| Класс | Назначение |
|---|---|
| Codec | Единые схема, экранирование, проверки и преобразования |
| PdbpWriter | Обычный AutoCloseable writer; создаёт новый файл, пишет заголовок и строки |
| PdbpProvider | TableProvider и DataSourceRegister, короткое имя pdbpline |
| PdbpTable | Table/SupportsRead; схема и единственная capability BATCH_READ |
| PdbpScan | Scan и Batch; схема результата и планирование |
| PdbpPartition | Сериализуемые путь и схема одной партиции |
| PdbpReaderFactory | Создаёт reader внутри Spark task |
| PdbpReader | Построчное чтение, GenericInternalRow; STRING превращается в UTF8String |

Сначала Spark находит провайдер через META-INF/services/org.apache.spark.sql.sources.DataSourceRegister.
inferSchema читает только заголовок. getTable проверяет схему повторно, ScanBuilder создаёт Scan,
Batch планирует одну InputPartition. Фабрика на исполнителе открывает файл, проверяет заголовок,
next() читает ровно одну строку, get() возвращает текущую запись. close() закрывает файл.
Весь набор данных на драйвере не буферизуется. Spark сам выполняет фильтр и агрегацию;
pushdown не реализован. Изменять файл во время выполнения запроса нельзя.

Официальный контракт: [TableProvider](https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/sql/connector/catalog/TableProvider.html),
[PartitionReader](https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/sql/connector/read/PartitionReader.html).

## Запуск из терминала

Установите JDK 17 и задайте JAVA_HOME. Gradle установленным быть не обязан.

~~~powershell
$env:JAVA_HOME = 'путь-к-JDK-17'
.\gradlew.bat clean build
.\gradlew.bat run
~~~

На Linux/macOS: chmod +x gradlew; ./gradlew clean build run.
Первый запуск скачивает зависимости из Maven Central. Интернет нужен при отсутствии кэша.
Модульные JVM options Java 17 уже заданы в build.gradle для test/run и сгенерированных start scripts.
При запуске main напрямую нужно перенести applicationDefaultJvmArgs в VM options.
На Windows может появиться предупреждение о winutils/native Hadoop: для используемого java.nio reader
оно не мешает чтению локального файла и Spark SQL, Hadoop-кластер не нужен.

## IntelliJ IDEA

1. File → Open → папка проекта → открыть как Gradle-проект.
2. Project SDK: JDK 17; Settings → Build Tools → Gradle → Gradle JVM: JDK 17, distribution: Wrapper.
3. Дождитесь синхронизации Gradle.
4. Выберите сохранённую конфигурацию DemoApp (Gradle run), нажмите зелёный треугольник.
   Альтернатива: Gradle tool window → Tasks → application → run.
5. Для тестов: Gradle → Tasks → verification → test.

Конфигурация запуска использует Gradle, поэтому JVM options и classpath берутся из build.gradle.
Абсолютных пользовательских путей в исходниках и конфигурации нет.

## Демонстрация

DemoApp создаёт уникальный файл в build/demo через PdbpWriter с 12 записями и запускает local[*].
printSchema показывает id: long, name: string, amount: double. show(false) показывает все 12 записей,
включая кириллицу и экранированные символы. Фильтр amount > 60 возвращает id 7–12 (6 записей).
groupBy(name).sum(amount) даёт:

| name | total |
|---|---:|
| Группа 0 | 300.0 |
| Группа 1 | 210.0 |
| Группа 2 | 260.0 |
| Иван с особыми символами | 10.0 |

explain(true) содержит BatchScan и PdbpBatchScan. Последняя строка:
VERIFIED: count=12, total=780.0, amount>60=6.
DemoApp проверяет count и total программно и останавливает Spark в finally.

## Тесты

.\gradlew.bat test — реальные локальные Spark-запросы, без моков.
Проверяются заголовок, writer, round-trip, типы InternalRow и Row, короткое имя,
SQL-фильтр/сумма, BatchScan в плане, пустой набор, альтернативная схема, UTF-8,
экранирование, lifecycle reader, неверные типы writer, повреждённые заголовки,
неверное число колонок, переполнение LONG/DOUBLE и некорректные числа/escape.
Параметризованные негативные сценарии также выполняют collectAsList(), чтобы действительно прочитать данные.
Отчёт: build/reports/tests/test/index.html. Ошибки в негативных тестах ожидаемы.

## Ограничения

Один существующий локальный файл (путь ОС или file: URI), одна партиция, только batch.
Нет streaming, каталога, транзакций, Spark V2 write, column pruning и filter pushdown.
Это учебный последовательный reader; параллелизм чтения ограничен одной партицией.
Для распределённого кластера файл пришлось бы сделать доступным исполнителям или добавить общий storage.
Docker не требуется и конфигурация Docker намеренно не включена.
PdbpWriter не перезаписывает существующие файлы; при ошибке может остаться частичный файл.
