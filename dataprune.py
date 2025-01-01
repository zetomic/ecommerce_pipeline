import pandas as pd

def main():
    # Load the CSV file
    df = pd.read_csv('ecommerce_clean.csv', engine='python')

    # Drop the specific columns
    columns_to_drop = ['Unnamed: 0_x', 'Unnamed: 0.2', 'Unnamed: 0.1', 'Unnamed: 0_y']
    df = df.drop(columns=columns_to_drop)

    # Drop rows with null values
    df = df.dropna()

    # Save the cleaned dataframe back to a CSV file
    df.to_csv('ecommerce.csv', index=False)

if __name__ == '__main__':
    main()
