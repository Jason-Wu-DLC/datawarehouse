

CREAT
TABLE restaurant (
    id SERIAL,
    name VARCHAR(60),
    address VARCHAR(80),
    city VARCHAR(30)
);

\copy restaurant(id, name, address, city) FROM 'DataLinkage_py/data/restaurant.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

GRANT SELECT ON TABLE "restaurant" TO "a2";

GRANT SELECT ON TABLE "sales" TO "a2";

-- three dimension tables, including “Staff”, “Product”, and “Time_Period”.

CREATE TABLE Staff (
    SID INT PRIMARY KEY,
    FNAME VARCHAR(20),
    LNAME VARCHAR(20),
    STATE VARCHAR(10),
    STORE VARCHAR(10)
);

CREATE TABLE Product (
    PID INT PRIMARY KEY,
    PRODUCT VARCHAR(40),
    BRAND VARCHAR(40),
    UNIT_COST DECIMAL(10, 2)
);

CREATE TABLE Time_Period (
    DATE DATE PRIMARY KEY,
    Month INT,
    Quarter INT,
    Year INT
);

CREATE TABLE Sales_Facts (
    TID INT PRIMARY KEY,
    SID INT,
    PID INT,
    DATE DATE,
    QUANTITY INT,
    PRICE DECIMAL(10, 2),
    FOREIGN KEY (SID) REFERENCES Staff (SID),
    FOREIGN KEY (PID) REFERENCES Product (PID),
    FOREIGN KEY (DATE) REFERENCES Time_Period (DATE)
);

Drop table Sales_Facts;

Drop table Time_Period;

Drop table Product;

Drop table Staff;

Drop table sales;

CREATE TABLE sales (
    TID INT,
    SID INT,
    FNAME VARCHAR,
    LNAME VARCHAR,
    STATE VARCHAR,
    STORE VARCHAR,
    DATE DATE,
    PID INT,
    BRAND VARCHAR,
    PRODUCT VARCHAR,
    UNIT_COST DECIMAL,
    QUANTITY INT,
    PRICE DECIMAL
);

\copy sales FROM '/home/s4565901/Sales.csv' WITH CSV HEADER;

INSERT INTO
    Staff (
        SID,
        FNAME,
        LNAME,
        STATE,
        STORE
    )
SELECT DISTINCT
    SID,
    FNAME,
    LNAME,
    STATE,
    STORE
FROM sales;

INSERT INTO
    Product (
        PID,
        PRODUCT,
        BRAND,
        UNIT_COST
    )
SELECT DISTINCT
    PID,
    PRODUCT,
    BRAND,
    UNIT_COST
FROM sales;

INSERT INTO
    Time_Period (DATE, Year, Quarter, Month)
SELECT DISTINCT
    DATE,
    EXTRACT(
        YEAR
        FROM DATE
    ) AS Year,
    EXTRACT(
        QUARTER
        FROM DATE
    ) AS Quarter,
    EXTRACT(
        MONTH
        FROM DATE
    ) AS Month
FROM sales;

INSERT INTO
    Sales_Facts (
        TID,
        SID,
        PID,
        QUANTITY,
        PRICE,
        DATE
    )
SELECT TID, SID, PID, QUANTITY, PRICE, DATE
FROM sales;

SELECT COUNT(*) AS Unique_Staff_Members FROM Staff;

SELECT COUNT(*) AS Transactions_2022_Qtr3
FROM
    Sales_Facts SF
    JOIN Time_Period TP ON SF.DATE = TP.DATE
WHERE
    TP.Year = 2022
    AND TP.Quarter = 3;

--cube

CREATE MATERIALIZED VIEW Sales_Time_Staff AS
SELECT Staff.STORE, Staff.STATE, Time_Period.Year, Time_Period.Quarter, SUM(
        Sales_Facts.QUANTITY * (
            Sales_Facts.PRICE - Product.UNIT_COST
        )
    ) AS Total_Profit
FROM
    Sales_Facts
    JOIN Staff ON Sales_Facts.SID = Staff.SID
    JOIN Time_Period ON Sales_Facts.Date = Time_Period.DATE
    JOIN Product ON Sales_Facts.PID = Product.PID
GROUP BY
    CUBE (
        Staff.STORE,
        Staff.STATE,
        Time_Period.Year,
        Time_Period.Quarter
    );

--DROP MATERIALIZED VIEW IF EXISTS Sales_Time_Staff;

--new

CREATE VIEW State_Sales_By_Quarter_2021 AS
SELECT st.STATE, st.Year, st.Quarter, st.Total_Profit
FROM Sales_Time_Staff st
WHERE
    st.Year = 2021
    AND st.STORE IS NULL
    AND st.STATE IS NOT NULL
    AND st.Quarter IS NOT NULL;

--DROP VIEW IF EXISTS State_Sales_By_Quarter_2021;

CREATE VIEW State_Revenue_Annual AS
SELECT STATE, Year, Total_Profit
FROM Sales_Time_Staff
WHERE (
        Year = 2021
        OR Year = 2022
        OR Year = 2023
    )
    AND store IS NULL
    AND STATE IS NOT NULL
    AND quarter IS NULL
    AND Year IS NOT NULL;

--DROP VIEW IF EXISTS State_Revenue_Annual;

CREATE MATERIALIZED VIEW Sales_Product_Staff AS
SELECT
    Staff.STORE,
    Staff.STATE,
    Product.PRODUCT,
    Product.BRAND,
    SUM(Sales_Facts.QUANTITY) AS TOTAL_QUANTITY,
    SUM(
        Sales_Facts.QUANTITY * (
            Sales_Facts.PRICE - Product.UNIT_COST
        )
    ) AS TOTAL_PROFIT,
    SUM(
        Sales_Facts.QUANTITY * Sales_Facts.PRICE
    ) AS GROSS
FROM
    Sales_Facts
    JOIN Staff ON Sales_Facts.SID = Staff.SID
    JOIN Product ON Sales_Facts.PID = Product.PID
GROUP BY
    CUBE (
        Staff.STORE,
        Staff.STATE,
        Product.PRODUCT,
        Product.BRAND
    );

--DROP MATERIALIZED VIEW IF EXISTS Sales_Product_Staff;

CREATE VIEW Top_3_Stores AS
SELECT STORE, STATE, GROSS, STORE_RANK
FROM (
        SELECT
            STORE, STATE, GROSS, DENSE_RANK() OVER (
                ORDER BY GROSS DESC
            ) AS STORE_RANK
        FROM Sales_Product_Staff
        WHERE
            STORE IS NOT NULL
            AND STATE IS NOT NULL
    ) AS ranked_stores
WHERE
    STORE_RANK <= 3;

-- DROP VIEW IF EXISTS Top_3_Stores;

CREATE VIEW Most_Profitable_Item_Per_Store AS
SELECT
    STORE,
    STATE,
    PRODUCT,
    BRAND,
    TOTAL_PROFIT
FROM (
        SELECT
            STORE, STATE, PRODUCT, BRAND, TOTAL_PROFIT, RANK() OVER (
                PARTITION BY
                    STORE, STATE
                ORDER BY TOTAL_PROFIT DESC
            ) AS PRODUCT_RANK
        FROM Sales_Product_Staff
        WHERE
            STORE IS NOT NULL
            AND STATE IS NOT NULL
            AND PRODUCT IS NOT NULL
            AND BRAND IS NOT NULL
    ) AS ranked_products
WHERE
    PRODUCT_RANK = 1;

--DROP VIEW IF EXISTS Most_Profitable_Item_Per_Store;