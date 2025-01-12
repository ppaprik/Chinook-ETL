# Chinook

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z AmazonBooks datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich čitateľských preferencií na základe hodnotení kníh a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---

## 1. Úvod a popis zdrojových dát

Cieľom semestrálneho projektu je analyzovať dáta v databáze Chinook, pričom sa zameriame na používateľov, ich preferencie a a kúpi skladieb . Táto analýza umožní identifikovať trendy v záujmoch používateľov, najpopulárnejšie položky (napríklad skladby alebo albumy) a správanie používateľov.
Dataset obsahuje tabulky:

- `playlist`: Informácie o playlistoch vytvorených užívatelmi.
- `playlisttrack`: Spojovacia tabuľka pre playlisty a skladby.
- `track`: Informácie o skladbách.
- `album`: Informácie o hudobných albumoch.
- `artist`: Informácie o interpretoch.
- `customer`: Informácie o zákazníkoch.
- `employee`: Informácie o zamestnancoch.
- `genre`: Informácie o žánroch skladieb.
- `invoice`: Informácie o fakturach a predajoch.
- `invoiceline`: Dodatočné informácie k faktúram.
- `mediatype`: Dodatočné informácie o type média skladby.

---

## 1.1 Dátová architektúra
### ERD diagram

Dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<img src="https://github.com/ppaprik/Chinook-ETL/blob/main/Chinook_ERD/Chinook_ERD.png" width="720"/>

---

## 2 Dimenzionálny model

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_invoice`**, ktorá je prepojená s nasledujúcimi dimenziami:

- **`dim_track`**: Zahŕna údaje o skladbách , albumoch , interpretoch a žánroch.
- **`dim_customer`**: Obsahuje informácie o zákazníkoch, ktorí vykonali nákupy.
- **`dim_employee`**: Obsahuje informácie o zamestnancoch, ktorí sa podieľali na transakciách.
- **`dim_address`**: Táto tabuľka obsahuje informácie o geografických lokalitách.
- **`dim_date`**: Táto tabuľka poskytuje podrobnosti o čase a dátumoch pre analýzu.

<img src="https://github.com/ppaprik/Chinook-ETL/blob/main/Chinook_SCD/Chinook_SCD.png" width="720"/>

---

## 3. ETL proces v Snowflake

ETL proces v Snowflake pozostával z troch hlavných fáz: extrahovanie (Extract), transformácia (Transform) a načítanie (Load). Tento proces slúžil na spracovanie zdrojových dát zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---

## 3.1 Extract (Extrahovanie dát)

**Príklad kódu:**:
```sql
CREATE OR REPLACE STAGE PENGUIN_CHINOOK_STAGE;
```

Do stage boli následne nahraté súbory obsahujúce údaje o knihách, používateľoch, hodnoteniach, zamestnaniach a úrovniach vzdelania. Dáta boli importované do staging tabuliek pomocou príkazu COPY INTO. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO artist_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('artist.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);
```

---

## 3.2 Transfor (Transformácia dát)

Transformácia dát zahŕňa prípravu a spracovanie dát do požadovanej formy pred ich načítaním do cieľových tabuliek. V tomto prípade bol vytvorený SQL dopyt na vytvorenie tabuľky dim_track, ktorá obsahuje rôzne atribúty skladieb. Ďalej boli údaje vložené z staging tabuliek, pričom bola aplikovaná logika na transformáciu rôznych atribútov ako dĺžka skladby, veľkosť súboru alebo cena.

```sql
CREATE OR REPLACE TABLE dim_track (
    dim_track_id INT AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    composer STRING,
    milliseconds INT,
    len VARCHAR(20) NOT NULL,
    bytes INT NOT NULL,
    size VARCHAR(20) NOT NULL,
    unit_price DECIMAL(10, 2),
    price_category VARCHAR(20) NOT NULL,
    media_type VARCHAR(120) NOT NULL,
    genre VARCHAR(120) NOT NULL,
    album VARCHAR(160) NOT NULL,
    artist VARCHAR(120) NOT NULL
);
```

Táto tabuľka obsahuje stĺpce ako názov skladby, skladateľa, dĺžku (v milisekundách), veľkosť súboru, cenu a kategórie ako cena alebo žáner.

```sql
INSERT INTO dim_track (name, composer, milliseconds, len, bytes, size, unit_price, price_category, media_type, genre, album, artist)
SELECT DISTINCT t.name,
    t.composer,
    t.millisecond,
    CASE 
        WHEN t.millisecond < 180000 THEN 'short'
        WHEN t.millisecond BETWEEN 180000 AND 300000 THEN 'medium'
        ELSE 'long'
    END AS len,
    t.bytes,
    CASE 
        WHEN t.bytes < 5000000 THEN 'small'
        WHEN t.bytes BETWEEN 5000000 AND 20000000 THEN 'medium'
        ELSE 'big'
    END AS size,
    t.unit_price,
    CASE 
        WHEN t.unit_price < 1.00 THEN 'cheap'
        ELSE 'expensive'
    END AS price_category,
    m.name AS media_type,
    g.name AS genre,
    a.title AS album,
    ar.name AS artist
FROM track_staging t
JOIN mediatype_staging m ON t.media_type_id = m.media_type_id
JOIN genre_staging g ON t.genre_id = g.genre_id
JOIN album_staging a ON t.album_id = a.album_id
JOIN artist_staging ar ON a.artist_id = ar.artist_id;
```

V tomto kroku sú údaje zo staging tabuliek track_staging, mediatype_staging, genre_staging, album_staging, a artist_staging spracované a vložené do cieľovej tabuľky dim_track. Používajú sa podmienky na transformáciu dĺžky skladby na kategórie ako "short", "medium" alebo "long", veľkosti súborov na "small", "medium" alebo "big", a ceny na "cheap" alebo "expensive".

Rovnaký prístup bol aplikovaný na všetky ostatné zdrojové dáta, pričom pre každý súbor bola vytvorená štruktúrovaná staging tabuľka, ktorá obsahovala nepretransformované dáta. Tento proces bol následne vykonaný aj pre ďalšie dimenzie a faktové tabuľky.

---

## 3.3 Load (Načítanie dát)

Po úspešnom vytvorení dimenzií a faktových tabuliek bolo potrebné optimalizovať úložisko odstránením staging tabuliek. 
Tým sa zabezpečilo, že nebudú zbytočne zaberať miesto v databáze. Následne sa vykonalo čistenie staging tabuliek.

```sql
DROP TABLE IF EXISTS artist_staging;
DROP TABLE IF EXISTS album_staging;
DROP TABLE IF EXISTS customer_staging;
DROP TABLE IF EXISTS employee_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS mediatype_staging;
DROP TABLE IF EXISTS playlist_staging;
DROP TABLE IF EXISTS track_staging;
DROP TABLE IF EXISTS playlisttrack_staging;
DROP TABLE IF EXISTS invoice_staging;
DROP TABLE IF EXISTS invoiceline_staging;
```

Týmto spôsobom sa uvoľnilo miesto v databáze a pripravili sa optimalizované tabulky pre ďalšie spracovanie a analýzu.

---

## 4 Vizualizácia dát

Dashboard obsahuje 5 vizualizácií, ktoré zobrazuje rôzne aspekty predajov a trendov v databáze Chinook.

<img src="https://github.com/ppaprik/Chinook-ETL/blob/main/graphs/dashboard.png" width="720"/>

---

### Graf 1: Predaje podľa žánru

Tento graf zobrazuje počet predajov rozdelených podľa žánru. Použitý SQL dopyt sa zameriava na počet predajov pre každý žáner hudby v databáze.

```sql
SELECT t.genre, COUNT(*) AS Sales_Count
FROM dim_track t
JOIN fact_invoice fi ON t.dim_track_id = fi.dim_track_id
GROUP BY t.genre;
```
<img src="https://github.com/ppaprik/Chinook-ETL/blob/main/graphs/SalesPerGenre.png" width="512"/>

---

### Graf 2: Cena distribúcie

Tento graf zobrazuje distribúciu cien podľa kategórií a množstva predaných položiek. Využíva sumu množstiev predaných za jednotlivé ceny a kategórie.

```sql
SELECT unit_price as Price, price_category, SUM(quantity) AS Quantity
FROM fact_invoice
GROUP BY unit_price, price_category;
```
<img src="https://github.com/ppaprik/Chinook-ETL/blob/main/graphs/PriceDistribution.png" width="512"/>

---

### Graf 3: Priemerná dĺžka hudby

Tento graf zobrazuje priemernú dĺžku skladieb podľa ich kategórie. Priemerné trvanie skladieb je vypočítané v minútach.

```sql
SELECT dt.len, ROUND((AVG(dt.milliseconds) / 1000 / 60), 2) AS Lenght
FROM fact_invoice fi
JOIN dim_track dt ON fi.dim_track_id = dt.dim_track_id
GROUP BY dt.len;

```
<img src="https://github.com/ppaprik/Chinook-ETL/blob/main/graphs/AverageLenghtOfSong.png" width="512"/>

---

### Graf 4: Predaje za všetky mesiace

Tento graf zobrazuje počet predajov za každý mesiac v roku. Dopyt sumarizuje počet predajov podľa mesiacov, zoradených podľa dátumu.

```sql
SELECT dd.month_as_string AS month, COUNT(fi.fact_invoice_id) AS sales_count, dd.months AS Mesiac
FROM fact_invoice fi
JOIN dim_date dd ON fi.dim_date_id = dd.date_id
GROUP BY dd.month_as_string, dd.months
ORDER BY dd.months ASC;
```
<img src="https://github.com/ppaprik/Chinook-ETL/blob/main/graphs/SalesPerAllMonth.png" width="512"/>

---

### Graf 5: Predaje za celý rok

Tento graf ukazuje celkový počet predajov za každý rok. Je to agregovaný pohľad na ročné predaje v rámci celej databázy.

```sql
SELECT dd.years, COUNT(*) AS Sales_Count
FROM fact_invoice t
JOIN dim_date dd ON dd.date_id = t.dim_date_id
GROUP BY dd.years;
```
<img src="https://github.com/ppaprik/Chinook-ETL/blob/main/graphs/SalesPerYear.png" width="512"/>

---

Autor: Patrik Šabo

