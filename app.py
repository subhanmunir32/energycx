import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd



db_config = {
    'dbname': 'x',
    'user': 'x',
    'password': 'x',
    'host': 'x',
    'port': 'x'
}


try:
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor(cursor_factory=RealDictCursor)  # Using RealDictCursor for a dict-like cursor
    print("Connection to the database was successful")

    
    query = """
    SELECT f.quote_date, AVG(f.price) as avg_price
    FROM fixed_prices_atc as f
    LEFT JOIN utility_to_iso_mapping as ui ON f.id = ui.iso_zone_id
    LEFT JOIN utility_key_id as uk on uk.utility_id = ui.utility_id
    WHERE uk.salesforce_id = 'a0G1H00000zvKSZUA2'
    AND f.start_month BETWEEN '2024-07-01' AND '2024-07-01'::date + INTERVAL '1 Year'
    AND f.quote_date BETWEEN '2021-01-01' and '2024-06-01'
    GROUP BY f.quote_date
    ORDER BY f.quote_date DESC
    LIMIT 20;
    """

    
    cursor.execute(query)

    # Fetch all results
    results = cursor.fetchall()
    
    df=pd.DataFrame(results)

    print (df.head())

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()

