import pandas as pd
import mysql.connector
from mysql.connector import Error

customers = pd.read_csv(r"C:\Users\THASIM SAMMIL\Downloads\Customers.csv", encoding='ISO-8859-1')
products = pd.read_csv(r"C:\Users\THASIM SAMMIL\Downloads\Products.csv")
sales = pd.read_csv(r"C:\Users\THASIM SAMMIL\Downloads\Sales.csv")
stores = pd.read_csv(r"C:\Users\THASIM SAMMIL\Downloads\Stores.csv")
exchange_Rates = pd.read_csv(r"C:\Users\THASIM SAMMIL\Downloads\Exchange_Rates.csv")


products['Unit Cost USD'] = products['Unit Cost USD'].replace('[\$,]', '', regex=True).astype(float)
products['Unit Price USD'] = products['Unit Price USD'].replace('[\$,]', '', regex=True).astype(float)

if 'State Code' in customers.columns:
    customers['State Code'] = customers['State Code'].fillna(customers['State Code'].mode()[0])

if pd.api.types.is_datetime64_any_dtype(sales['Delivery Date']):
    median_date = sales['Delivery Date'].median()
    sales['Delivery Date'] = sales['Delivery Date'].fillna(median_date)
else:
    sales['Delivery Date'] = pd.to_datetime(sales['Delivery Date'], errors='coerce')
    median_date = sales['Delivery Date'].median()
    sales['Delivery Date'] = sales['Delivery Date'].fillna(median_date)

if stores['Square Meters'].isnull().sum() > 0:
    stores['Square Meters'] = stores['Square Meters'].fillna(stores['Square Meters'].median())

date_columns = ['Order Date', 'Delivery Date', 'Open Date', 'Date', 'Birthday']
for df in [customers, sales, stores, exchange_Rates]:
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')


customers = customers.drop_duplicates(subset='CustomerKey', keep='last')
products = products.drop_duplicates(subset='ProductKey', keep='last')
stores = stores.drop_duplicates(subset='StoreKey', keep='last')

def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root', 
            password='12345678',
            database='Global_Electronics_DB'
        )
        if conn.is_connected():
            print("Connected to MySQL database")
        return conn
    except Error as e:
        print(f"Error: {e}")
        return None

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Customers (
        CustomerKey INT PRIMARY KEY,
        Gender VARCHAR(10),
        Name VARCHAR(255),
        City VARCHAR(255),
        StateCode VARCHAR(25),
        State VARCHAR(255),
        ZipCode VARCHAR(10),
        Country VARCHAR(255),
        Continent VARCHAR(50),
        Birthday DATE
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Products (
        ProductKey INT PRIMARY KEY,
        ProductName VARCHAR(255),
        Brand VARCHAR(255),
        Color VARCHAR(50),
        UnitCostUSD DECIMAL(10, 2),
        UnitPriceUSD DECIMAL(10, 2),
        SubcategoryKey INT,
        Subcategory VARCHAR(255),
        CategoryKey INT,
        Category VARCHAR(255)
    );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Stores (
        StoreKey INT PRIMARY KEY,
        Country VARCHAR(255),
        State VARCHAR(255),
        SquareMeters DECIMAL(10, 2),
        OpenDate DATE
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Sales (
        OrderNumber INT PRIMARY KEY,
        LineItem INT,
        OrderDate DATE,
        DeliveryDate DATE,
        CustomerKey INT,
        StoreKey INT,
        ProductKey INT,
        Quantity INT,
        CurrencyCode VARCHAR(10),
        FOREIGN KEY (CustomerKey) REFERENCES Customers(CustomerKey),
        FOREIGN KEY (StoreKey) REFERENCES Stores(StoreKey),
        FOREIGN KEY (ProductKey) REFERENCES Products(ProductKey)
    );
    """)
  
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ExchangeRates (
        Date DATE,
        CurrencyCode VARCHAR(10),
        ExchangeRate DECIMAL(10, 4),
        PRIMARY KEY (Date, CurrencyCode)
    );
    """)
    conn.commit()
    cursor.close()
    print("Tables created successfully!")

def insert_data_to_table(df, table_name, conn):
    cursor = conn.cursor()
    for i, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        sql = f"INSERT IGNORE INTO {table_name} VALUES ({placeholders})"
        try:
            cursor.execute(sql, tuple(row))
        except mysql.connector.Error as e:
            print(f"Error inserting row {i}: {e}")
            print(f"Data: {tuple(row)}")
            continue
    conn.commit()
    cursor.close()
    print(f"Data inserted successfully into {table_name}!")

conn = connect_to_mysql()
if conn:
    create_tables(conn)
    insert_data_to_table(customers, 'Customers', conn)
    insert_data_to_table(products, 'Products', conn)
    insert_data_to_table(sales, 'Sales', conn)
    insert_data_to_table(stores, 'Stores', conn)
    insert_data_to_table(exchange_Rates, 'ExchangeRates', conn)
    conn.close()
    print("All data inserted successfully!")
else:
    print("Failed to connect to the database.")