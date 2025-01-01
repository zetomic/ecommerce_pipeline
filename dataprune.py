import pandas as pd

def main():
    # Load the CSV files
    sales_df = pd.read_csv('Sales.csv', engine='python')
    products_df = pd.read_csv('products.csv', engine='python')

    # Join the dataframes on 'product_id'
    df = pd.merge(sales_df, products_df, on='product_id')

    # Drop the specific columns
    columns_to_drop = ['Unnamed: 0_x', 'Unnamed: 0.2', 'Unnamed: 0.1', 'Unnamed: 0_y']
    df = df.drop(columns=columns_to_drop)

    # Drop rows with null values
    df = df.dropna()

    # Save the cleaned dataframe back to a CSV file
    df.to_csv('ecommerce.csv', index=False)

if __name__ == '__main__':
    main()
