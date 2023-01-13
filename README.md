# Csv to PostgreSql with PHP

Converte CSV para PostgreSql.

## Install with composer
```
composer require the.moiza/csvtopostgresql
```

## CONTRIBUTE

Join this project: https://discord.com/channels/973324521987792916/973324521987792918

Support this project: https://skit.network/donate

## PT-BR

Dependências da ferramenta: **php7.2 ou maior**, **mbstring**, **fileinfo**, **zip** e **pdo**;

Você poder usar o modo CLI para fazer a conversão, ou criar sua própria implementação.

```
php cli.php
```

### CONFIGURAÇÕES

- **createPkey** = Se true, cria crave primária **\_pkey\_**, padrão: **false**;
- **enableTrim** = Se true, corta espaços em branco, quebra de linha tab no início e fim de cada dado importado, padrão: **true**;
- **enableTransaction** = Se true habilita boa prática de Transaction (recomendado), padrão: **true**;
- **justCreateTables** = Se true, não insere os dados, apenas cria as tabelas, padrão: **false**;
- **inputEncoding** = Seta encoding do CSV, padrão: UTF-8';
- **outputEncoding** = Seta encoding do banco de dados, padrão: UTF-8';

### O CSV

O CSV deve estar em formato ZIP.

O(s) CSV(s) devem ter na primeira linha os nomes das colunas. Cada CSV será importado como uma tabela.

O nome da tabela será o nome do arquivo CSV corretamente tratado com lowcase, remoção de acentos, substituição de espaços (&nbsp;) e hífen (-) por (_) underline;

Os nomes das colunas serão tratados com as mesmas regras.

Binários (blob, bytea) devem ser exportados em BASE64 para o CSV e serão importados como **text**.

Exemplo de CSV aceito:

|LatD|LatM|LatS|NS|LonD|LonM|LonS|EW|City      |State|
|----|----|----|--|----|----|----|--|----------|-----|
|41  |5   |59  |N |80  |39  |0   |W |Youngstown|OH   |
|42  |52  |48  |N |97  |23  |23  |W |Yankton   |SD   |
|46  |35  |59  |N |120 |30  |36  |W |Yakima    |WA   |

O separador de coluna será detectado automaticamente, pode ser ,(vírgula) ou ;(ponto e vírgula).

A ferramenta tentará detectar os principais tipos de dados:

- timestamp
- date
- numeric
- integer
- boolean
- text

## EN

Help us to translate and write the documentation in other languages.