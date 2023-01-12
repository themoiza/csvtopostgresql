# Csv to PostgreSql with PHP

### Install with composer
```
composer require the.moiza/csvtopostgresql
```

## PT-BR

Converta CSV para PostgreSql, o CSV deve estar em formato ZIP, sem pastas dentro do zip.

Esta ferramenta necessita das dependêncis: **php-pdo, php-zip**;

Você poder usar o modo CLI para fazer a conversão, ou criar sua própria implementação.

```bash
php cli.php
```

Esta ferramenta tenta detectar os principais tipos de dados:

- timestamp
- date
- numeric
- integer
- boolean
- text

## EN

Help us to translate and write the documentation in other languages.