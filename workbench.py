# %%
%load_ext kedro.ipython
%reload_kedro .
io = catalog
io.list()
pd.set_option('display.max_rows', 100)
# %%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import ScalarFormatter

cleaned_df = io.load("cleaned_results")
# Remove any line with more than 400,000 km mileage (outliers)
cleaned_df = cleaned_df[cleaned_df['mileage'] <= 400000]
len(cleaned_df)
# %%
# %%
# Basic Stats

# Replace 'data' with your actual data list. Here, it is assumed df is the DataFrame
df = cleaned_df

# Display basic statistics
print(df.describe())

# Check for missing values
print(df.isnull().sum())

# Display the first few rows of the DataFrame
print(df.head())


# %%
# Distribution of Car Prices
plt.figure(figsize=(10, 6))
sns.histplot(df['price'], bins=15, kde=True)
plt.title('Distribution of Car Prices')
plt.xlabel('Price (€)')
plt.ylabel('Frequency')
plt.show()

# %%
# Create the plot
plt.figure(figsize=(10, 6))
sns.histplot(df['mileage'], bins=15, kde=True)
plt.title('Distribution of Mileage')
plt.xlabel('Mileage (km)')
plt.ylabel('Frequency')

# Ensure the x-axis does not use scientific notation
ax = plt.gca()
ax.get_xaxis().get_major_formatter().set_useOffset(False)
ax.get_xaxis().set_major_formatter(ScalarFormatter())

plt.show()
# %%
df

# %%
plt.figure(figsize=(10, 6))
df['year'] = df['first_registration'].dt.year
sns.countplot(x='year', data=df)
plt.title('Count of Cars by Year of First Registration')
plt.xlabel('Year of First Registration')
plt.ylabel('Count')
plt.show()

# %%
plt.figure(figsize=(10, 6))
sns.histplot(df['engine_power'], bins=15, kde=True)
plt.title('Distribution of Engine Power')
plt.xlabel('Engine Power (kW)')
plt.ylabel('Frequency')
plt.show()

# %%
len(df)
# %%
plt.figure(figsize=(10, 6))
sns.histplot(df['co2_emission'].dropna(), bins=45, kde=True)
plt.title('Distribution of CO2 Emissions')
plt.xlabel('CO2 Emission (g/km)')
plt.ylabel('Frequency')
plt.show()

# %%
# Adding a hypothetical 'country' column for demonstration purposes

df.groupby('country')['price'].describe()
plt.figure(figsize=(12, 6))
sns.boxplot(x='country', y='price', data=df)
plt.title('Comparison of Car Prices by Country')
plt.xlabel('Country')
plt.ylabel('Price (€)')
plt.show()


# %%
plt.figure(figsize=(12, 6))
sns.boxplot(x='subtitle', y='price', hue='country', data=df)
plt.title('Comparison of Car Prices by Model and Country')
plt.xlabel('Car Model')
plt.ylabel('Price (€)')
plt.xticks(rotation=90)
plt.legend(title='Country')
plt.show()

# %%
# linear regression analysis

# Assuming cleaned_df is your DataFrame
df = cleaned_df.copy()
# Check for missing data and handle appropriately
df = df.dropna(subset=['price', 'country']) # Example handling of missing data
# One-hot encode categorical variables
df = pd.get_dummies(df, columns=['transmission', 'brand', 'fuel_type', 'model'], drop_first=True, dtype=int)

# Ensure the 'country' is treated correctly as a binary variable.
df['country'] = df['country'].apply(lambda x: 1 if x == 'Netherlands' else 0)

# Check the dataframe
df.dtypes

import statsmodels.api as sm

# Define the independent variables (adding a constant for intercept)
# Convert the 'first_registration' column to int
df['first_registration'] = df['first_registration'].dt.year.astype(int)
df = df.dropna(subset=['engine_power', 'price', 'country']) # Example handling of missing data
X = df.drop(columns=['price', 'url', 'subtitle', 'first_registration', 'fuel_consumption', 'co2_emission', 'vat_deductible', 'html'])
X = sm.add_constant(X)  # Adds a constant term to the predictor

# Define the dependent variable
y = df['price']

# Fit the model
model = sm.OLS(y, X).fit()

# Get the summary of the model
print(model.summary())

# %%
#==============================
#========== visualise diff
#==============================
import matplotlib.pyplot as plt
import seaborn as sns

def plot_price_vs_mileage(df, model_name):
    """
    Plot a scatter plot showing the price vs mileage of the specified car model.
    
    Args:
    df (pd.DataFrame): DataFrame containing car data.
    model_name (str): Name of the car model to filter and plot.
    """
    # Filter for the specified model
    df_model = df[df['model'].str.contains(model_name, case=False, na=False)]

    # Calculate the age of the vehicle
    df_model['year'] = df_model['first_registration'].dt.year
    df_model['age'] = 2024 - df_model['year']

    # Plotting
    plt.figure(figsize=(12, 8))
    scatter_plot = sns.scatterplot(
        data=df_model, 
        # x='mileage', y='price', 
        x='mileage', y='price', 
        # size='age', 
        hue='country', 
        # sizes=(20, 200), 
        palette='viridis', 
        alpha=0.4, 
        edgecolor='w', 
        legend='brief'
    )
        # Calculate the average price for each mileage for each country
    avg_price_by_mileage = df_model.groupby(['mileage', 'country'])['price'].mean().reset_index()

    # Add smoothed lines for each country
    for country in df_model['country'].unique():
        sns.regplot(
            data=df_model[df_model['country'] == country], 
            x='mileage', y='price', 
            scatter=False, 
            label=f'{country} (smoothed)',
            lowess=True, 
            line_kws={'linewidth': 2}
        )


    # Customize the plot
    plt.title(f'{model_name.capitalize()}: Price vs Mileage of Vehicle by Country')
    plt.xlabel('Mileage (km)')
    plt.ylabel('Price (€)')
    scatter_plot.legend(title='Country')
    scatter_plot.grid(True)

    # Show plot
    plt.show()

# Example usage
plot_price_vs_mileage(cleaned_df, 'focus')
plot_price_vs_mileage(cleaned_df, 'octavia')
plot_price_vs_mileage(cleaned_df, 'fiesta')
plot_price_vs_mileage(cleaned_df, 'golf')
# %%
plot_price_vs_mileage(cleaned_df, 'ceed')
plot_price_vs_mileage(cleaned_df, 'a3')
plot_price_vs_mileage(cleaned_df, '500')
# %%
