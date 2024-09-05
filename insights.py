import mysql.connector

connection = mysql.connector.connect(
    host='localhost',
    user='root', 
    password='12345678',
    database='Global_Electronics_DB'
)

cursor = connection.cursor()

queries = {
    "TotalSalesByProduct": """
       CREATE TABLE IF NOT EXISTS TotalSalesByProduct AS
    SELECT 
        Products.ProductName, 
        SUM(Sales.Quantity) AS TotalQuantity, 
        SUM(Products.UnitPriceUSD * Sales.Quantity) AS TotalSales
    FROM Sales
    JOIN Products ON Sales.ProductKey = Products.ProductKey
    GROUP BY Products.ProductName;
    """,
    "SalesTrendsOverTime": """
        CREATE TABLE IF NOT EXISTS SalesTrendsOverTime AS
    SELECT 
    DATE_FORMAT(Sales.OrderDate, '%Y-%m-%d') AS Month, 
    SUM(Products.UnitPriceUSD * Sales.Quantity) AS MonthlySales
    FROM Sales
    JOIN Products ON Sales.ProductKey = Products.ProductKey
    GROUP BY Month
    ORDER BY Month;
    """,
    "Top10CustomersBySales": """
            CREATE TABLE IF NOT EXISTS Top10CustomersBySales AS
    SELECT 
        Customers.CustomerKey, 
        SUM(Products.UnitPriceUSD * Sales.Quantity) AS TotalSales
    FROM Sales
    JOIN Customers ON Sales.CustomerKey = Customers.CustomerKey
    JOIN Products ON Sales.ProductKey = Products.ProductKey
    GROUP BY Customers.CustomerKey
    ORDER BY TotalSales DESC
    LIMIT 10;
    """,
    "StorePerformanceAnalysis": """
                CREATE TABLE IF NOT EXISTS StorePerformanceAnalysis AS
        SELECT 
            Sales.StoreKey, 
            Stores.Country, 
            SUM(Products.UnitPriceUSD * Sales.Quantity) AS TotalSales
        FROM Sales
        JOIN Stores ON Sales.StoreKey = Stores.StoreKey
        JOIN Products ON Sales.ProductKey = Products.ProductKey
        GROUP BY Sales.StoreKey, Stores.Country;
    """,
    "SalesByRegion": """
        CREATE TABLE IF NOT EXISTS SalesByRegion AS
        SELECT Country, SUM(Products.UnitPriceUSD * Sales.Quantity) AS TotalSales
        FROM Sales
        JOIN Stores ON Sales.StoreKey = Stores.StoreKey
        JOIN Products ON Sales.ProductKey = Products.ProductKey
        GROUP BY Country;
    """,
    "ProductSalesDistribution": """
        CREATE TABLE IF NOT EXISTS ProductSalesDistribution AS
        SELECT Products.ProductName, SUM(Products.UnitPriceUSD * Sales.Quantity) AS TotalSales
        FROM Sales
        JOIN Products ON Sales.ProductKey = Products.ProductKey
        GROUP BY ProductName
        ORDER BY TotalSales DESC;
    """,
    "AverageSalesPerStore": """
        CREATE TABLE IF NOT EXISTS AverageSalesPerStore AS
        SELECT Sales.StoreKey, AVG(Products.UnitPriceUSD * Sales.Quantity) AS AvgSales
        FROM Sales
                JOIN Products ON Sales.ProductKey = Products.ProductKey
        GROUP BY StoreKey;
    """,
    "CustomerPurchaseFrequency": """
        CREATE TABLE IF NOT EXISTS CustomerPurchaseFrequency AS
        SELECT CustomerKey, COUNT(DISTINCT OrderNumber) AS PurchaseCount
        FROM Sales
        GROUP BY CustomerKey;
    """,
    "ExchangeRateImpactOnSales": """
        CREATE TABLE IF NOT EXISTS ExchangeRateImpactOnSales AS
        SELECT Sales.CurrencyCode, AVG(ExchangeRate) AS AvgExchangeRate, SUM(Products.UnitPriceUSD * Sales.Quantity) AS TotalSales
        FROM Sales
        JOIN ExchangeRates ON Sales.CurrencyCode = ExchangeRates.CurrencyCode
        JOIN Products ON Sales.ProductKey = Products.ProductKey
        GROUP BY Sales.CurrencyCode;
    """,
    "SalesByProductCategory": """
        CREATE TABLE IF NOT EXISTS SalesByProductCategory AS
        SELECT Category, SUM(UnitPriceUSD * Quantity) AS TotalSales
        FROM Sales
        JOIN Products ON Sales.ProductKey = Products.ProductKey
        GROUP BY Category;
    """
}


try:
    for name, query in queries.items():
        print(f"Executing query: {name}")
        cursor.execute(query)
        connection.commit()
        print(f"Table '{name}' created successfully.")
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    cursor.close()
    connection.close()
    print("Database connection closed.")
