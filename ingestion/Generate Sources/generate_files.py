# Databricks notebook source
import urllib
import pandas as pd
import numpy as np



def generar_productos (engine):
    
    query_all_productos = """
        SELECT
            P.ProductKey AS product_id,
            PC.EnglishProductCategoryName AS category,
            PS.EnglishProductSubcategoryName AS subcategory,        
            P.ModelName AS brand,
            P.EnglishProductName AS description,         
            P.ListPrice AS price     
        FROM dbo.DimProduct AS P
        LEFT JOIN dbo.DimProductSubcategory AS PS
            ON P.ProductSubcategoryKey = PS.ProductSubcategoryKey
        LEFT JOIN dbo.DimProductCategory AS PC
            ON PS.ProductCategoryKey = PC.ProductCategoryKey
        WHERE P.FinishedGoodsFlag = 1;
    """
    df_productos = pd.read_sql(query_all_productos, engine)

    # REGLA DE INTEGRIDAD: Sincronizar el precio con la transformación de ventas
    price_base = df_productos["price"].astype(float)

    condiciones = [
        price_base < 100,
        price_base < 500,
        price_base < 1000,
        price_base < 3000,
    ]

    opciones_price = [
        price_base,
        price_base / 2,
        price_base / 4,
        price_base / 6,
    ]

    # Aplicamos la misma reducción al catálogo de productos
    df_productos["price"] = np.select(
        condiciones, opciones_price, default=df_productos["price"]
    )

    return df_productos


def generar_sales(engine):
    sales_query = """
        SELECT
            concat(F.SalesOrderNumber,F.ProductKey) AS sale_id,
            F.CustomerKey AS customer_id,
            F.ProductKey AS product_id,
            D.FullDateAlternateKey AS [timestamp],
            F.OrderQuantity AS quantity,
            F.UnitPrice AS unit_price,
            F.SalesAmount AS sales_amount
        FROM dbo.FactInternetSales AS F
        INNER JOIN dbo.DimDate AS D
            ON F.OrderDateKey = D.DateKey;
        """

    # 1. Extraer el DataFrame original desde la base de datos
    sales_lines_df = pd.read_sql(sales_query, engine)
    
    sales_lines_df["timestamp"] = pd.to_datetime(sales_lines_df["timestamp"])
    sales_lines_df["timestamp"] = sales_lines_df["timestamp"] + pd.to_timedelta(
        4626, unit="D"
    )

    sales_lines_df["timestamp"] = sales_lines_df["timestamp"].dt.date
   

    # 2. Definir las condiciones basadas en el unit_price original
    price_base = sales_lines_df["unit_price"].astype(float)

    condiciones = [
        price_base < 100,
        price_base < 500,
        price_base < 1000,
        price_base < 3000,
    ]

    # 3. Definir las transformaciones para cantidad y precio
    opciones_qty = [1, 2, 4, 6]

    opciones_price = [
        price_base,
        price_base / 2,
        price_base / 4,
        price_base / 6,
    ]

    # 4. Aplicar la lógica vectorial (si no cumple ninguna, default mantiene el original)
    sales_lines_df["quantity"] = np.select(
        condiciones, opciones_qty, default=sales_lines_df["quantity"]
    )
    sales_lines_df["unit_price"] = np.select(
        condiciones, opciones_price, default=sales_lines_df["unit_price"]
    )

    # 5. Forzar el recalculo exacto para evitar inconsistencias por decimales
    sales_lines_df["sales_amount"] = (
        sales_lines_df["quantity"] * sales_lines_df["unit_price"]
    )

    return sales_lines_df   


def generar_customers(engine): 

    customers_query= """ 
        SELECT
            C.CustomerKey AS customer_id,
            C.DateFirstPurchase AS signup_date,
        
            G.City AS city,
            G.StateProvinceName AS state,
            G.EnglishCountryRegionName AS country,
        
            CASE
                WHEN C.NumberChildrenAtHome > 0
                    THEN 'Family'
                ELSE 'NoChildrenAtHome'
            END AS segment,
        
            C.BirthDate AS birth_date,
        
            CASE C.Gender
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'Unknown'
            END AS gender,
        
            CASE C.MaritalStatus
                WHEN 'M' THEN 'Married'
                WHEN 'S' THEN 'Single'
                ELSE 'Unknown'
            END AS marital_status,
        
            C.YearlyIncome AS yearly_income,
            C.TotalChildren AS total_children,
            C.NumberChildrenAtHome AS children_at_home,
        
            C.EnglishEducation AS education,
            C.EnglishOccupation AS occupation,
        
            C.HouseOwnerFlag AS house_owner,
            C.NumberCarsOwned AS number_cars
        
        FROM dbo.DimCustomer AS C
        
        LEFT JOIN dbo.DimGeography AS G
            ON C.GeographyKey = G.GeographyKey;
    """
    df_customers = pd.read_sql(customers_query, engine)

    df_customers["signup_date"] = pd.to_datetime(df_customers["signup_date"])
    df_customers["signup_date"] = df_customers[
        "signup_date"
    ] + pd.to_timedelta(4626, unit="D")

    df_customers["signup_date"] = df_customers["signup_date"].dt.date

    return df_customers

# COMMAND ----------

def main():
    import os
    from datetime import datetime, timedelta
    from getpass import getpass
    import pandas as pd
    from sqlalchemy import create_engine, text

    driver = "ODBC Driver 17 for SQL Server"
    server = getpass("Introduce el servidor: ")
    database = "AdventureWorksDW2019"
    trusted_connection = "yes"

    connection_string = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection={trusted_connection}"
    engine = create_engine(connection_string)

    print("⏳ Extrayendo y transformando datos desde SQL Server...")
    df_productos = generar_productos(engine)
    df_customers = generar_customers(engine)
    df_sales = generar_sales(engine)

    print("✂️ Aplicando filtros estrictos para simular carga incremental...")


    print(f"   - Ventas Históricas: {len(df_sales)} filas")
    print(f"   - Clientes Históricos: {len(df_customers)} filas")

    print("\n Exportando archivos físicos...")

    # EXPORTACIÓN DE VENTAS (PARQUET)
    df_sales.to_parquet(
        "data/sales.parquet", index=False, compression="snappy",allow_truncated_timestamps=True
    )

    # EXPORTACIÓN DE CLIENTES (CSV)
    df_customers.to_csv(
        "data/customers.csv", index=False, encoding="utf-8"
    )


    # EXPORTACIÓN DE PRODUCTOS (JSON - Se queda completo ya que el catálogo no cambia por fechas)
    df_productos.to_json(
        "data/products.json", orient="records", date_format="iso", indent=4
    )

    print("finalizado con éxito")

if __name__ == "__main__":
    main()