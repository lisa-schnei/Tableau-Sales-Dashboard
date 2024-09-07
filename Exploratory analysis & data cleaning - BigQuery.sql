
------------------------------------------
-- ADVENTURE WORKS : SALES DATA ANALYSIS -
------------------------------------------

-- Author: Lisa Schneider
-- Date: 2/06/2024
-- Tool used: BigQuery

-----------------------------------------
--------- EXPLORATORY ANALYSIS ----------
-----------------------------------------

-- 1. What is the monthly sales development over time, including total sales (revenue), order count and sales growth?
# While order count has increased massively since 2003-08, revenue has not followed suit in a similar pattern. Sales growth is also flunctuating. 

WITH monthly_revenue AS (
  SELECT FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
    ROUND(SUM(TotalDue)) AS revenue,
    COUNT(DISTINCT SalesOrderID) AS number_of_orders
  FROM `adwentureworks_db.salesorderheader`
  GROUP BY FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH))
  ORDER BY order_month
),

revenue_with_previous AS (
  SELECT order_month,
    revenue,
    monthly_revenue.number_of_orders,
    LAG(revenue) OVER (ORDER BY order_month) AS previous_revenue
  FROM monthly_revenue
)

SELECT order_month,
  revenue,
  previous_revenue,
  CASE WHEN previous_revenue IS NULL THEN NULL
    ELSE ROUND((revenue - previous_revenue) / previous_revenue * 100, 2)
  END AS revenue_growth_percentage,
  number_of_orders
FROM revenue_with_previous
ORDER BY order_month;

-- 2. What is the development of average order value over time?
# AOV shows a negative trend over time with a distinct change and drop from 2003-08 when order count increased.

WITH monthly_revenue AS (
  SELECT FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
    ROUND(SUM(TotalDue)) AS revenue,
    COUNT(DISTINCT SalesOrderID) AS number_of_orders
  FROM `adwentureworks_db.salesorderheader`
  GROUP BY FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH))
  ORDER BY order_month
),

revenue_with_previous AS (
  SELECT order_month,
    revenue,
    monthly_revenue.number_of_orders,
    LAG(revenue) OVER (ORDER BY order_month) AS previous_revenue
  FROM monthly_revenue
)

SELECT order_month,
  revenue,
  previous_revenue,
  CASE WHEN previous_revenue IS NULL THEN NULL
    ELSE ROUND((revenue - previous_revenue) / previous_revenue * 100, 2)
  END AS revenue_growth_percentage,
  number_of_orders,
  ROUND(revenue / number_of_orders, 2) AS average_order_value
FROM revenue_with_previous
ORDER BY order_month;

-- 3. How are sales split (and developing) between product categories, markets and channels?

# Split by markets over time
SELECT FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(s.OrderDate, MONTH)) AS order_month,
  t.CountryRegionCode,
  t.Name AS region,
  COUNT(s.SalesOrderID) AS number_of_orders,
  ROUND(SUM(s.TotalDue)) AS total_amount 
FROM `adwentureworks_db.salesorderheader` as s
LEFT JOIN `adwentureworks_db.salesterritory` AS t USING (TerritoryID)
GROUP BY FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(s.OrderDate, MONTH)), 
  t.CountryRegionCode, t.Name
ORDER BY t.CountryRegionCode, 
  t.Name, order_month;


# Sales by category and subcategory: 4 main categories and 37 subcategories; 2 subcategories with no sales
SELECT productcat.Name As category_name
, productsubcat.Name AS subcategory_name
, SUM(salesorder.TotalDue) AS total_revenue
, (SUM(salesorder.TotalDue)) / COUNT(salesorder.SalesOrderID) AS average_order_vale
FROM `adwentureworks_db.salesorderheader` AS salesorder
LEFT JOIN `adwentureworks_db.salesorderdetail` AS salesdetail 
  ON salesorder.SalesOrderID = salesdetail.SalesOrderID
LEFT JOIN `adwentureworks_db.product` AS product
  ON salesdetail.ProductID = product.ProductID
LEFT JOIN `adwentureworks_db.productsubcategory` AS productsubcat
  ON product.ProductSubcategoryID = productsubcat.ProductSubcategoryID
LEFT JOIN `adwentureworks_db.productcategory` AS productcat
  ON productcat.ProductCategoryID = productsubcat.ProductCategoryID
GROUP BY category_name, subcategory_name
ORDER BY category_name, subcategory_name;

# Monthly sales and order count by sales channel - strong growth in order count for online from 2003-08 but not equal growth for store channel
SELECT FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
  CASE WHEN SalesPersonID IS NULL THEN 'ONLINE' ELSE 'STORE' END AS channel,
  COUNT(SalesOrderID) AS number_of_orders,
  ROUND(SUM(TotalDue)) AS total_amount
FROM `adwentureworks_db.salesorderheader` 
GROUP BY ALL;

-- 4. What are the top and bottom selling products by sales volume and revenue?
# Bikes not so prominent in the top 10 list of orders by count nor revenue which is slightly unexpected. 
SELECT product.Name
, COUNT(salesorder.SalesOrderID) AS order_count
, SUM(salesorder.TotalDue) AS revenue
FROM `adwentureworks_db.salesorderheader` AS salesorder
LEFT JOIN `adwentureworks_db.salesorderdetail` AS salesdetail 
  ON salesorder.SalesOrderID = salesdetail.SalesOrderID
LEFT JOIN `adwentureworks_db.product` AS product
  ON salesdetail.ProductID = product.ProductID
LEFT JOIN `adwentureworks_db.productsubcategory` AS productsubcat
  ON product.ProductSubcategoryID = productsubcat.ProductSubcategoryID
LEFT JOIN `adwentureworks_db.productcategory` AS productcat
  ON productcat.ProductCategoryID = productsubcat.ProductCategoryID
GROUP BY product.Name
ORDER BY SUM(salesorder.TotalDue) ASC
# ORDER BY SUM(salesorder.TotalDue) ASC
LIMIT 10;

-- 5. Who are the top 10 most valuable customers by revenue?

SELECT contact.ContactId
, contact.Firstname
, contact.LastName
, SUM(salesorder.TotalDue) AS total_revenue
FROM `adwentureworks_db.salesorderheader` AS salesorder 
LEFT JOIN `adwentureworks_db.contact` AS contact
  ON salesorder.ContactID = contact.ContactId
GROUP BY ALL
ORDER BY total_revenue DESC
LIMIT 10;

-- 6. Who are the top 10 most selling sales persons by revenue?

SELECT employee.EmployeeId 
, contact.Firstname
, contact.LastName
, SUM(salesorder.TotalDue) AS total_revenue
FROM `adwentureworks_db.salesorderheader` AS salesorder 
LEFT JOIN `adwentureworks_db.employee` AS employee
  ON salesorder.SalesPersonID = employee.EmployeeId
LEFT JOIN `adwentureworks_db.contact` AS contact
  ON employee.ContactID = contact.ContactId
GROUP BY ALL
ORDER BY total_revenue DESC
LIMIT 10;


-----------------------------------------
------------ PREPARING DATA -------------
-----------------------------------------


SELECT SalesOrderID
, OrderDate
, FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month
, DueDate
, ShipDate
, Status
, CustomerID
, ContactID
, SalesPersonID
, CASE WHEN SalesPersonID IS NULL THEN 'ONLINE' ELSE 'STORE' END AS channel
, TerritoryID
, TotalDue
FROM `adwentureworks_db.salesorderheader`;

SELECT TerritoryID
, Name
, CountryRegionCode
, salesterritory.Group
, SalesYTD
, SalesLastYear
FROM `adwentureworks_db.salesterritory` AS salesterritory;

SELECT SalesOrderID
, SalesOrderDetailID
, OrderQty
, ProductID
, SpecialOfferID
, UnitPrice
, UnitPriceDiscount
, LineTotal
FROM `adwentureworks_db.salesorderdetail`;

SELECT ProductID
, Name
, ProductNumber
, Color
, StandardCost
, ListPrice
, Size
, ProductSubcategoryID
, ProductModelID
, SellStartDate
, SellEndDate
, DiscontinuedDate
FROM `adwentureworks_db.product`;

SELECT ProductCategoryID
, Name
FROM `adwentureworks_db.productcategory`;

SELECT ProductSubCategoryID
, ProductCategoryID
, Name
FROM `adwentureworks_db.productsubcategory`;

SELECT ContactID
, FirstName
, MiddleName
, LastName
, CONCAT(Firstname, ' ', MiddleName, ' ', LastName) AS full_name
FROM `adwentureworks_db.contact`;

SELECT EmployeeID
, ContactID
, Title
FROM `adwentureworks_db.employee`;


SELECT salesorderheadersalesreason.SalesOrderID
, salesreason.SalesReasonID
, salesreason.Name
, salesreason.ReasonType
FROM `adwentureworks_db.salesorderheadersalesreason` AS salesorderheadersalesreason
LEFT JOIN `adwentureworks_db.salesreason` AS salesreason
  ON salesorderheadersalesreason.SalesReasonID = salesreason.SalesReasonID;
  
