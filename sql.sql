----------------------------------------------------------------------------------------------------
-- Create database, use that database, use a public schema

CREATE DATABASE PENGUIN_CHINOOK;
USE DATABASE PENGUIN_CHINOOK;


----------------------------------------------------------------------------------------------------
-- Create and use staging scheme

CREATE OR REPLACE SCHEMA PENGUIN_CHINOOK.staging;
USE SCHEMA PENGUIN_CHINOOK.staging;


----------------------------------------------------------------------------------------------------
-- Create tables into staging scheme

CREATE OR REPLACE TABLE artist_staging (
    artist_id INT AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(120) NOT NULL
);

CREATE OR REPLACE TABLE album_staging (
    album_id INT AUTOINCREMENT PRIMARY KEY,
    title VARCHAR(160) NOT NULL,
    artist_id INT NOT NULL,
    FOREIGN KEY (artist_id) REFERENCES artist_staging(artist_id)
);

CREATE OR REPLACE TABLE genre_staging (
    genre_id INT AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(120)
);

CREATE OR REPLACE TABLE mediatype_staging (
    media_type_id INT AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(120) NOT NULL
);

CREATE OR REPLACE TABLE playlist_staging (
    playlist_id INT AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(120)
);

CREATE OR REPLACE TABLE track_staging (
    track_id INT AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    album_id INT NOT NULL,
    media_type_id INT NOT NULL,
    genre_id INT NOT NULL,
    composer VARCHAR(220),
    millisecond INT NOT NULL,
    bytes INT NOT NULL,
    unit_price DECIMAL (10, 2) NOT NULL,
    FOREIGN KEY (album_id) REFERENCES album_staging(album_id),
    FOREIGN KEY (media_type_id) REFERENCES mediatype_staging(media_type_id),
    FOREIGN KEY (genre_id) REFERENCES genre_staging(genre_id)
);

CREATE OR REPLACE TABLE playlisttrack_staging (
    playlist_id INT NOT NULL,
    track_id INT NOT NULL,
    FOREIGN KEY (playlist_id) REFERENCES playlist_staging(playlist_id),
    FOREIGN KEY (track_id) REFERENCES track_staging(track_id),
    PRIMARY KEY (playlist_id, track_id)
);


CREATE OR REPLACE TABLE employee_staging (
    employee_id INT AUTOINCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    title VARCHAR(200) NOT NULL,
    reports_to INT,
    birth_date DATE NOT NULL,
    hire_date DATE NOT NULL,
    address STRING NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50),
    country VARCHAR(50) NOT NULL,
    postal_code VARCHAR(25),
    phone VARCHAR(30),
    fax VARCHAR(30),
    email VARCHAR(60) NOT NULL,
    FOREIGN KEY (reports_to) REFERENCES employee_staging(employee_id)
);

CREATE OR REPLACE TABLE customer_staging (
    customer_id INT AUTOINCREMENT PRIMARY KEY,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    company VARCHAR(80),
    address VARCHAR(70),
    city VARCHAR(40),
    state VARCHAR(40),
    country VARCHAR(40),
    postal_code VARCHAR(10),
    phone VARCHAR(24),
    fax VARCHAR(24),
    email VARCHAR(60) NOT NULL,
    support_rep_id INT NOT NULL,
    FOREIGN KEY (support_rep_id) REFERENCES employee_staging(employee_id)
);

CREATE OR REPLACE TABLE invoice_staging (
    invoice_id INT AUTOINCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    invoice_date DATE NOT NULL,
    billing_address VARCHAR(70),
    billing_city VARCHAR(40),
    billing_state VARCHAR(40),
    billing_country VARCHAR(40),
    billing_postal_code VARCHAR(10),
    total DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customer_staging(customer_id)
);

CREATE OR REPLACE TABLE invoiceline_staging (
    invoice_line_id INT AUTOINCREMENT PRIMARY KEY,
    invoice_id INT NOT NULL,
    track_id INT NOT NULL,
    unit_price DECIMAL (10, 2) NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY (invoice_id) REFERENCES invoice_staging(invoice_id),
    FOREIGN KEY (track_id) REFERENCES track_staging(track_id)
);


----------------------------------------------------------------------------------------------------
-- Create a stage into staging scheme

CREATE OR REPLACE STAGE PENGUIN_CHINOOK_STAGE;

-- Create a File Format (easier copying data into tables)
CREATE OR REPLACE FILE FORMAT UTF_8_CSV_FILE_FORMAT
    TYPE = CSV
    COMPRESSION = NONE
    FIELD_DELIMITER = ','
    FILE_EXTENSION = 'csv'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    SKIP_HEADER = 1;

    
----------------------------------------------------------------------------------------------------
-- Copy data from .csv files into tables in staging scheme

COPY INTO artist_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('artist.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO album_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('album.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO customer_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('customer.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO employee_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('employee.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO genre_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('genre.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO mediatype_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('mediatype.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO playlist_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('playlist.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO track_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('track.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO playlisttrack_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('playlisttrack.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO invoice_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('invoice.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);

COPY INTO invoiceline_staging
FROM @PENGUIN_CHINOOK_STAGE/
FILES = ('invoiceline.csv')
FILE_FORMAT = (FORMAT_NAME = UTF_8_CSV_FILE_FORMAT);


----------------------------------------------------------------------------------------------------
-- Creating the SCD dimension tables

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

CREATE OR REPLACE TABLE dim_employee (
    employee_id INT AUTOINCREMENT PRIMARY KEY,
    last_name VARCHAR(50) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    title VARCHAR(200) NOT NULL,
    birth_date DATE NOT NULL,
    age_group VARCHAR(15),
    hire_date DATE NOT NULL,
    address STRING NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    postal_code VARCHAR(25),
    phone VARCHAR(30) NOT NULL,
    fax VARCHAR(30) NOT NULL,
    email VARCHAR(60) NOT NULL
);

CREATE OR REPLACE TABLE dim_customer (
    customer_id INT AUTOINCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    company VARCHAR(50),
    address STRING NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50),
    country VARCHAR(50) NOT NULL,
    postal_code VARCHAR(25),
    phone VARCHAR(30),
    fax VARCHAR(30),
    email VARCHAR(60) NOT NULL
);

CREATE OR REPLACE TABLE dim_date (
    date_id INT AUTOINCREMENT PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    years INT NOT NULL,
    months INT NOT NULL,
    month_as_string VARCHAR(20) NOT NULL,
    days INT NOT NULL,
    day_as_string VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE OR REPLACE TABLE dim_address (
    dim_address_id INT AUTOINCREMENT PRIMARY KEY,
    billing_address STRING NOT NULL,
    billing_city VARCHAR(50) NOT NULL,
    billing_state VARCHAR(50),
    billing_country VARCHAR(50) NOT NULL,
    billing_postal_code VARCHAR(25)
);

CREATE OR REPLACE TABLE fact_invoice (
    fact_invoice_id INT AUTOINCREMENT PRIMARY KEY,
    unit_price DECIMAL(10, 2) NOT NULL,
    price_category VARCHAR(20) NOT NULL,
    quantity INT NOT NULL,
    dim_address_id INT NOT NULL,
    dim_track_id INT NOT NULL,
    dim_customer_id INT NOT NULL,
    dim_employee_id INT NOT NULL,
    dim_date_id INT NOT NULL,
    FOREIGN KEY (dim_address_id) REFERENCES dim_address(dim_address_id),
    FOREIGN KEY (dim_track_id) REFERENCES dim_track(dim_track_id),
    FOREIGN KEY (dim_customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (dim_employee_id) REFERENCES dim_employee(employee_id),
    FOREIGN KEY (dim_date_id) REFERENCES dim_date(date_id)
);


----------------------------------------------------------------------------------------------------
-- Insert data into tables

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

INSERT INTO dim_employee (last_name, first_name, title, birth_date, age_group, hire_date, address, city, state, country, postal_code, phone, fax, email)
    SELECT last_name,
        first_name,
        title,
        birth_date,
        CASE 
            WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birth_date) <= 17 THEN 'young'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birth_date) BETWEEN 18 AND 59 THEN 'Adult'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birth_date) >= 60 THEN 'Senior'
        END AS age_group,
        hire_date,
        address,
        city,
        state,
        country,
        postal_code,
        phone,
        fax,
        email
    FROM employee_staging;

INSERT INTO dim_customer (first_name, last_name, company, address, city, state, country, postal_code, phone, fax, email)
    SELECT first_name,
        last_name,
        company,
        address,
        city,
        state,
        country,
        postal_code,
        phone,
        fax,
        email
    FROM customer_staging;

INSERT INTO dim_address (billing_address, billing_city, billing_state, billing_country, billing_postal_code)
    SELECT billing_address, 
        billing_city,
        billing_state,
        billing_country,
        billing_postal_code
    FROM invoice_staging;

INSERT INTO dim_date (timestamp, years, months, month_as_string, days, day_as_string, is_weekend)
    SELECT invoice_date AS timestamp,
        EXTRACT(YEAR FROM invoice_date) AS years,
        EXTRACT(MONTH FROM invoice_date) AS months,
        CASE 
            WHEN EXTRACT(MONTH FROM invoice_date) = 1 THEN 'January'
            WHEN EXTRACT(MONTH FROM invoice_date) = 2 THEN 'February'
            WHEN EXTRACT(MONTH FROM invoice_date) = 3 THEN 'March'
            WHEN EXTRACT(MONTH FROM invoice_date) = 4 THEN 'April'
            WHEN EXTRACT(MONTH FROM invoice_date) = 5 THEN 'May'
            WHEN EXTRACT(MONTH FROM invoice_date) = 6 THEN 'June'
            WHEN EXTRACT(MONTH FROM invoice_date) = 7 THEN 'July'
            WHEN EXTRACT(MONTH FROM invoice_date) = 8 THEN 'August'
            WHEN EXTRACT(MONTH FROM invoice_date) = 9 THEN 'September'
            WHEN EXTRACT(MONTH FROM invoice_date) = 10 THEN 'October'
            WHEN EXTRACT(MONTH FROM invoice_date) = 11 THEN 'November'
            WHEN EXTRACT(MONTH FROM invoice_date) = 12 THEN 'December'
        END AS month_as_string,
        EXTRACT(DAY FROM invoice_date) AS days,
        CASE 
            WHEN EXTRACT(DOW FROM invoice_date) = 0 THEN 'Sunday'
            WHEN EXTRACT(DOW FROM invoice_date) = 1 THEN 'Monday'
            WHEN EXTRACT(DOW FROM invoice_date) = 2 THEN 'Tuesday'
            WHEN EXTRACT(DOW FROM invoice_date) = 3 THEN 'Wednesday'
            WHEN EXTRACT(DOW FROM invoice_date) = 4 THEN 'Thursday'
            WHEN EXTRACT(DOW FROM invoice_date) = 5 THEN 'Friday'
            WHEN EXTRACT(DOW FROM invoice_date) = 6 THEN 'Saturday'
        END AS day_as_string,
        CASE WHEN EXTRACT(DOW FROM invoice_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM invoice_staging;

INSERT INTO fact_invoice (unit_price, price_category, quantity, dim_address_id, dim_track_id, dim_customer_id, dim_employee_id, dim_date_id)
    SELECT il.unit_price,
        CASE 
            WHEN il.unit_price < 1.00 THEN 'cheap' 
            ELSE 'expensive'
        END AS price_category,
        il.quantity, 
        da.dim_address_id,
        dt.dim_track_id,
        dc.customer_id,
        de.employee_id,
        dd.date_id
    FROM invoiceline_staging il
    JOIN invoice_staging i ON il.invoice_id = i.invoice_id
    JOIN customer_staging c ON i.customer_id = c.customer_id
    JOIN employee_staging e ON c.support_rep_id = e.employee_id
    JOIN dim_address da ON i.billing_address = da.billing_address AND i.billing_city = da.billing_city AND i.billing_state = da.billing_state AND i.billing_country = da.billing_country AND i.billing_postal_code = da.billing_postal_code
    JOIN dim_track dt ON il.track_id = dt.dim_track_id
    JOIN dim_customer dc ON i.customer_id = dc.customer_id
    JOIN dim_employee de ON c.support_rep_id = de.employee_id
    JOIN dim_date dd ON i.invoice_date = dd.timestamp;

    
----------------------------------------------------------------------------------------------------
-- Dropping old staging tables

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