# Arenadata Elasticsearch Ranger Security Plugin

> sdk install java 15.0.1.hs-adpt

> Apache Maven 3.6.3 and higher

> mvn clean compile package -DskipTests=true

> java -jar target/Jetty-1.0-SNAPSHOT.jar

__

TODOs

__

+ Проверить и записать шаги установки плагина.
  
  java -jar JettyClient.jar проверка наполнения локального кэша и правильного описания Ranger сервиса

  Elasticsearch: the name of the class to load: ADLSPlugin

+ Скорректировать работу для Ranger 2.0.1
  
  Плагин - переработанная версия 2.2.0 c поддержкой версии 2.0.1
  
  Совместимость с Elasticsearch 7.10.2
  
  Совместимость с Java 15 (сборка под Java 14) - основные сложности с Jetty (REST фреймворк)
  
  Требует подмены библиотеки jackson-core-2.12.2.jar в Open Distro Elasticsearch 1.13.2-1
  
  (/usr/share/elasticsearch/lib/jackson-core-2.10.4.jar удалить, jackson-core-2.12.2.jar добавить вместо неё)

+ Как проверять пароль, токен и другие варианты авторизации
  
  Изучить REST API Ranger
  
  Если нет необходимых методов - добавить новые от Arenadata

+ Open Distro объединение плагинов
  
+ Проверка работы плагина в OpenSearch

+ Тестирование разных политик в Ranger

+ Логирование к единому виду, аудит, настройки

+ Разработать пример [plugin] для HTTP сервиса

+ Разобраться с лицензиями и лицензированием
  
  __ 
