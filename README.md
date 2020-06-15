# Задание 

[Тестовое задание для python-разработчика](https://github.com/avtocod/python-developer-test-task)

# Решение

* Язык реализации `python3.6`
* Хранилище `PostgreSQL`

# Установка

```bash
$ git clone https://github.com/zexusx26/spider
$ cd spider
$ make build
```

# Тесты

Выполнить из корня проекта:

```bash
$ make start
$ make test
$ make shutdown
```

# Использование

Выполнить из корня проекта:

```bash
$ make start
$ docker-compose run --rm app ./app <commands>
```

## Запуск обходчика

Выполнить из корня проекта (предполагается, что команда `$ make start` выполнена):

```bash
$ docker-compose run --rm app ./app load <url> [--depth <depth>]
```

* `url` - URL, с которого начинается обход
* `depth` - глубина обхода, значение по-умолчанию 0

Например, для обхода сайта `https://ria.ru` с глубиной 1:

```bash
$ make start
$ docker-compose run --rm app ./app load https://ria.ru --depth 1
```

## Получение URL и заголовков

Выполнить из корня проекта (предполагается, что команда `$ make start` выполнена):

```bash
$ docker-compose run --rm app ./app get <url> -n <n>
```

* `url` - URL, по домену 2-го уровня которого будет фильтроваться вывод
* `n` - требуемое число записей, значение по-умолчанию 1


Например, для получения 25 страниц с сайта `https://ria.ru`:

```bash
$ docker-compose run --rm app ./app get https://ria.ru -n 25
```

## Завершение работы

Выполнить из корня проекта:

```bash
$ make shutdown
```

# Комментарии

1. Проект не для прода, так что есть определенные недостатки в плане безопасности.
2. Для оптимизации времени выполнения работы можно воспользоваться параллельной обработкой контента (скорее всего, для этого лучше всего подойдет реализация параллельных вычислений через процессы). Возможно, это приведет и к уменьшению пиковой потребляемой памяти (но это не точно). Не реализовано.
3. Изменен формат вывода прогруженных страниц (стрелка вместо двоеточия).
4. Скрипт не ходит на сайты, не относящиеся к базовому домену.
5. Тестами покрыты не все ветки.
6. Существующие страницы в БД перезаписываются при попытке обновления.
