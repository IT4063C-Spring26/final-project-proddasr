#!/usr/bin/env python
# coding: utf-8

# # {Final Project Climate Change}📝
# 
# ![Banner](./assets/banner.jpeg)

# ## Topic
# *What problem are you (or your stakeholder) trying to address?*
# 📝 <!-- Answer Below -->

# ### The topic I'm trying to address is climate change.

# ## Project Question
# *What specific question are you seeking to answer with this project?*
# *This is not the same as the questions you ask to limit the scope of the project.*
# 📝 <!-- Answer Below -->

# ### How do emissions relate to climate change trends?

# ## What would an answer look like?
# *What is your hypothesized answer to your question?*
# 📝 <!-- Answer Below -->

# ### Emissions relate to climate change trends because greenhouse gases, mainly carbon dioxide from burning fossil fuels, deforestation, and agriculture, act as a heat trapping blanket, which directly causes global warming and climate change.

# ## Data Sources
# *What 3 data sources have you identified for this project?*
# *How are you going to relate these datasets?*
# 📝 <!-- Answer Below -->
# For this project, I will be using data from the following data sources
# 
# - 1. https://ourworldindata.org/co2-and-greenhouse-gas-emissions#explore-data-on-co2-and-greenhouse-gas-emissions
# - 2. https://databank.worldbank.org/source/world-development-indicators
# - 3. The following datasets from Kaggle
#         - a.https://www.kaggle.com/datasets/ashishraut64/global-methane-emissions
#         - b.https://www.kaggle.com/datasets/ravindrasinghrana/carbon-co2-emissions
#         - c.https://www.kaggle.com/datasets/willianoliveiragibin/greenhouse-gas-emissions

# ## Approach and Analysis
# *What is your approach to answering your project question?*
# *How will you use the identified data to answer your project question?*
# 📝 <!-- Start Discussing the project here; you can add as many code cells as you need -->
# 
# My approach is to combine emissions data from multiple datasets and analyze trends over time. I will clean and merge the datasets using common variables such as country and year. The data will be stored and queried using a SQLite database. I will then create visualizations, such as line charts, to observe how emissions change over time and compare patterns across countries. This analysis will help determine how emissions relate to climate change trends.

# In[394]:


# Import necessary libraries for data manipulation and visualization.
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from pandas.plotting import scatter_matrix
from mpl_toolkits.mplot3d import Axes3D
from sklearn.pipeline import make_pipeline
from sklearn.metrics import mean_squared_error


# In[395]:


# Import the data from Our World in Data and store it in a DataFrame.

url = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"
df = pd.read_csv(url)

df.sample(5)


# In[397]:


# Create a new DataFrame with the relevant columns for distinct countries and assign a unique country_id to each country.
countries = df[["country"]].drop_duplicates().reset_index(drop=True)

# Assign a unique country_id to each country using the index of the DataFrame. Adding 1 to start the country_id from 1 instead of 0.
countries["country_id"] = countries.index + 1

countries.sample(10)


# In[398]:


# Merge the country_id back to the main DataFrame

df = df.merge(countries, on="country")
df.sample(10)


# In[399]:


# Create a new DataFrame with the relevant columns for emissions.
population = df[["country_id", "year", "population"]]
population.head()


# In[400]:


# Create a new DataFrame with the relevant columns for emissions.
emissions = df[["country_id", "year", "co2", "methane"]]

emissions.sample(5)


# In[401]:


# Store the DataFrames in a SQLite database.
conn = sqlite3.connect("climate.db")

countries.to_sql("countries", conn, if_exists="replace", index=False)
emissions.to_sql("emissions", conn, if_exists="replace", index=False)
population.to_sql("population", conn, if_exists="replace", index=False)


# In[402]:


# Write a SQL query to join the three tables and retrieve the country name, year, CO2 emissions, methane emissions, and population.
query = """
SELECT c.country, e.year, e.co2, e.methane, p.population
FROM emissions e
JOIN countries c ON e.country_id = c.country_id
JOIN population p ON e.country_id = p.country_id AND e.year = p.year
"""

database_result = pd.read_sql(query, conn)
database_result.sample(5)


# In[403]:


# Import the Indicator data downloaded from World Bank and store it in a DataFrame.
WorldBank_Development_Indicator_df = pd.read_table('WorldBank_Development_Indicators_Data.csv', sep=',')


# In[404]:


# Import data from csv files imported from Kaggle and store it in DataFrames.
Methane_final_df = pd.read_table('Methane_final.csv', sep=',')
# Drop the 'Unnamed: 0' column from the Methane_final_df DataFrame, which is a unnecessary index column that was created during the CSV export process.
Methane_final_df = Methane_final_df.drop(columns=['Unnamed: 0'])

Carbon_emissions_df = pd.read_table('Carbon_(CO2)_Emissions_by_Country.csv', sep=',')

Carbon_emissions_per_capita_df = pd.read_table('co-emissions-per-capita new.csv', sep=',')


# ## 1. Exploratory Data Analysis (EDA)

# In[405]:


# Perform basic data analysis on the database_result DataFrame to understand the structure and summary statistics of the data.
database_result.info()
database_result.describe()


# - My dataset from the database has yearly data for different countries showing the emission details like CO2 and methane and it also lists population.
# - CO2, methane, and population have a lot of missing data.

# In[406]:


# Checks for missing values in the database_result DataFrame.
database_result.isnull().sum()


# In[407]:


# Checking the data types of the columns in the database_result DataFrame.
print(database_result.dtypes)


# In[408]:


# Check for duplicates in the database_result DataFrame.
database_result.duplicated().sum()


# In[409]:


# Display a random sample of 5 rows from the WorldBank_Development_Indicator_df DataFrame to get an overview of the data.
WorldBank_Development_Indicator_df.sample(5)


# - The World Bank Development Indicators dataset contains Country Name, Series Name, and many year columns. Since the data is in a wide format, it needs to be reshaped into a clearer structure for easier analysis.

# In[410]:


# Reshape the WDI dataset from wide to long format by converting year columns into a single 'year' column,
# with corresponding indicator values stored in 'series_value'.
# Each row now represents a unique combination of country, year, and indicator.

wdi_reshaped = WorldBank_Development_Indicator_df.melt(
    id_vars=['Country Name', 'Country Code', 'Series Name', 'Series Code'],
    var_name='year',
    value_name='series_value'
)

wdi_reshaped.sample(5)


# In[411]:


# Perform basic data analysis on the wdi_reshaped DataFrame to understand the structure and summary statistics of the data.
wdi_reshaped.info()
wdi_reshaped.describe()


# - The dataset contains multiple countries and indicators across different years. After reshaping, each row now represents a country–indicator–year combination. However, all columns are currently stored as strings will need to change this during the cleaning stage.
# - The summary statistics also show only unique, top and freq indicating the data type correction needs to happen
# - The Year field has some inconsistent values which need to be cleaned

# In[412]:


# Perform basic data analysis on the Methane_final_df DataFrame to understand the structure and summary statistics of the data.
Methane_final_df.info()
Methane_final_df.describe()


# - My dataset for methane dataset shows methane emission details including the type segment etc. for each country per year
# 
# - This dataset doesn't have any missing values.

# In[413]:


# Checking for null values in the Methane_final_df DataFrame.
Methane_final_df.isnull().sum()


# In[414]:


# Check for duplicates in the Methane_final_df DataFrame.
Methane_final_df.duplicated().sum()


# In[415]:


# Checking the data types of the columns in the Methane_final_df DataFrame.
print(Methane_final_df.dtypes)


# In[416]:


# Perform basic data analysis on the Carbon_emissions_df DataFrame to understand the structure and summary statistics of the data.
Carbon_emissions_df.info()
Carbon_emissions_df.describe()


# - The dataset for carbon emissions dataframe contains C02 emission details for each country based on a date.
# - There is no null values in this dataset.
# - There is no year field so, I need to a create a year field from the date to be able to join to the other datasets.

# In[417]:


# Check a sample of the data from the carbon emissions dataset.
Carbon_emissions_df.head(5)


# In[418]:


# Checking for null values in the Carbon_emissions_df DataFrame.
Carbon_emissions_df.isnull().sum()


# In[419]:


# Check for duplicates in the Carbon_emissions_df DataFrame.
Carbon_emissions_df.duplicated().sum()


# In[420]:


# Checking the data types of the columns in the Carbon_emissions_df DataFrame.
print(Carbon_emissions_df.dtypes)


# In[421]:


# Perform basic data analysis on the Carbon_emissions_per_capita_df DataFrame to understand the structure 
# and summary statistics of the data.
Carbon_emissions_per_capita_df.info()
Carbon_emissions_per_capita_df.describe()


# - The carbon emissions per capita has data showing annual CO2 emissions per capita. For each entity(country) per year.
# - There are no null values for this dataset.

# In[422]:


# Check the data from the carbon emissions per capita dataset.
Carbon_emissions_per_capita_df.sample(5)


# In[423]:


# Checking for null values in the Carbon_emissions_per_capita_df DataFrame.
Carbon_emissions_per_capita_df.isnull().sum()


# In[424]:


# Check for duplicates in the Carbon_emissions_per_capita_df DataFrame.
Carbon_emissions_per_capita_df.duplicated().sum()


# In[425]:


# Checking the data types of the columns in the Carbon_emissions_per_capita_df DataFrame.
print(Carbon_emissions_per_capita_df.dtypes)


# In[426]:


# Create a bar plot to visualize the distribution of CO2 emissions in the result DataFrame.
import seaborn as sns
import matplotlib.pyplot as plt

sns.histplot(database_result['co2'], bins=10)
plt.title("Distribution of CO2 Emissions")
plt.xlabel("CO2 Emissions")
plt.ylabel("Frequency")
plt.show()


# ##### I prepared the above visualization using a bar plot to show the distribution of CO2 emissions across various countries.
# ##### The graph shows that the data is right-skewed meaning that few countries produce high emissions.

# In[427]:


# Create a line plot to visualize the average CO2 and methane emissions over time.
yearly = database_result.groupby('year')[['co2', 'methane']].mean().reset_index()

plt.plot(yearly['year'], yearly['co2'], label='CO2')
plt.plot(yearly['year'], yearly['methane'], label='Methane')

plt.title("Average Emissions of CO2 and Methane Over Time")
plt.xlabel("Year")
plt.ylabel("Emissions")
plt.legend()
plt.show()


# ##### The above line plot shows how both CO2 and Methane emissions have changed over time.
# ##### This graph helps us identify the increasing trends of the emissions.

# In[428]:


wdi_reshaped['Series Name'].value_counts().plot(kind='bar')


# - The bar chart shows the distribution of records across different indicators in the dataset. It helps identify how frequently each indicator appears after reshaping.

# In[429]:


# Create a bar plot to visualize the emissions by type.

region_emissions = Methane_final_df.groupby('type')['emissions'].mean()

plt.bar(region_emissions.index, region_emissions.values)

plt.title("Emissions by Type")
plt.xlabel("Type")
plt.ylabel("Emissions")
plt.xticks(rotation=45)

plt.show()


# - The above bar plot shows a different type of methane emissions.
# - Based on this graph I can tell that agriculture causes higher emissions than the rest.

# In[430]:


# Create a pie chart to visualize the share of CO2 emissions by country for the top 10 emitting countries.
top_co2 = Carbon_emissions_df.groupby('Country')['Kilotons of Co2'].sum().nlargest(10)

top_co2.plot(kind='pie', autopct='%1.1f%%')
plt.title("Top 10 CO2 Emissions Share by Country")
plt.show()


# ##### - The above pie chart shows emissions of the top 10 countries. This graph tells me that majority of the emsission are from these 10 countries.

# In[431]:


# Create a horizontal bar plot to visualize the top 10 entities by CO2 emissions per capita. 

#convert the 'Annual CO₂ emissions (per capita)' column to numeric.
Carbon_emissions_per_capita_df['Annual CO₂ emissions (per capita)'] = pd.to_numeric(
    Carbon_emissions_per_capita_df['Annual CO₂ emissions (per capita)'],
    errors='coerce'
)

top_pc = Carbon_emissions_per_capita_df.groupby('Entity')['Annual CO₂ emissions (per capita)'].mean().nlargest(10)

top_pc.sort_values().plot(kind='barh')
plt.title("Top 10 Entities by CO2 Emissions Per Capita")
plt.xlabel("CO2 Emissions Per Capita")
plt.ylabel("Entity")
plt.show()


# ##### - This graph shows the top 10 entities(countries) CO2 emissions per person. Rwanda is the highest in CO2 emissions per capita.

# ## 2. Data Cleaning and Transformation

# - Clean up the data in database_result dataframe

# In[432]:


#Check missing values in the database_result DataFrame.
database_result.isnull().sum()


# In[433]:


# Drop rows where CO2 is missing, because CO2 is an essential data point for our analysis.
database_result = database_result.dropna(subset=['co2'])

# Fill population with median
database_result['population'] = database_result['population'].fillna(database_result['population'].median())

# Fill methane with median.
database_result['methane'] = database_result['methane'].fillna(database_result['methane'].median())


# In[434]:


# Check again
database_result.isnull().sum()


# In[435]:


# Dropping missing values from the result DataFrame.
database_result.dropna(inplace=True)


# In[436]:


# Checking for null values after dropping them.
database_result.isnull().sum()


# In[437]:


# Checking the data types of the columns in the database_result DataFrame.
print(database_result.dtypes)


# In[438]:


# Checking the data types of the columns in the result DataFrame.
print(database_result.dtypes)


# ##### Check for any outliers in the database_result dataframe.

# In[439]:


# Create a box plot to visualize the distribution of CO2 emissions in the database_result DataFrame.
sns.boxplot(x=database_result['co2'])
plt.show()


# In[440]:


# Remove outliers from the result DataFrame based on the CO2 emissions column.
# Calculate the 99th percentile of the CO2 emissions to identify outliers and filter out 
# those values from the database_result DataFrame.
database_result = database_result[database_result['co2'] < database_result['co2'].quantile(0.99)]


# - Cleanup data in the World Bank Indicater DataFrame (Reshaped)

# In[441]:


# Cleanup the Year column in the wdi_reshaped DataFrame by extracting the year part. 
# The currrent Year column has a value like '2025 [YR2025]'.

wdi_reshaped['year'] = wdi_reshaped['year'].str.extract(r'(\d{4})')
wdi_reshaped.head()


# In[442]:


# Check the data types of the columns in the wdi_reshaped DataFrame.
wdi_reshaped.info()


# In[443]:


# Check the data in the wdi_reshaped DataFrame after cleaning the year column.
wdi_reshaped.sample(5)


# In[444]:


# Convert the data types of the wdi_reshaped DataFrame to appropriate types for analysis.
wdi_reshaped['year'] = wdi_reshaped['year'].astype(int)

# Convert the 'series_value' column to numeric, coercing errors to NaN. The data has some characters like '..' 
# which cannot be converted to numeric, so we will coerce those errors to NaN. This will allow us to perform 
# numerical analysis on this column.
wdi_reshaped['series_value'] = pd.to_numeric(
    wdi_reshaped['series_value'], errors='coerce'
)
wdi_reshaped.info()


# In[445]:


# Check the data after conversion.
wdi_reshaped.sample(5)


# In[446]:


# Check for missing values in the wdi_reshaped DataFrame after conversion.
wdi_reshaped.isna().sum()


# - Observing lot of missing values in the series_value column of the wdi_reshaped DataFrame after conversion, we need to remove them for better analysis

# In[447]:


# Removing the missing values from the wdi_reshaped DataFrame to ensure that our analysis is based on complete data.
wdi_cleaned = wdi_reshaped.dropna(subset=['series_value'])

wdi_cleaned.isna().sum()


# In[448]:


# Check the info of the wdi_cleaned DataFrame after removing missing values.
wdi_cleaned.info()


# In[449]:


# Check the data in the wdi_cleaned DataFrame after removing missing values.
wdi_cleaned.head()


# In[450]:


# Reset the index of the wdi_cleaned DataFrame after dropping missing values to ensure a clean index for analysis.
wdi_cleaned = wdi_cleaned.reset_index(drop=True)


# In[451]:


# Check the data in the wdi_cleaned DataFrame after resetting the index.
wdi_cleaned.head()


# In[452]:


# Rename the columns in Methane_final_df to standardize the data sets
wdi_cleaned = wdi_cleaned.rename(columns={
    'Country Name': 'country'
})

print(wdi_cleaned.columns)


# ##### Adding a few visualizations on the cleaned world data indicators data

# In[453]:


# Picking one indicater - GDP per capita (current US$) - from the wdi_cleaned DataFrame for further analysis.
gdp_data = wdi_cleaned[
    wdi_cleaned['Series Name'] == 'GDP per capita (current US$)'
]


# In[454]:


# Pick 3 countries for easier analysis and visualization.
countries = ['United States', 'India', 'China']

# Filter the gdp_data DataFrame to include only the selected countries for analysis.
gdp_data = gdp_data[gdp_data['country'].isin(countries)]


# In[455]:


plt.figure(figsize=(10,6))

for country in countries:
    subset = gdp_data[gdp_data['country'] == country]
    plt.plot(subset['year'], subset['series_value'], label=country)

plt.xlabel('Year')
plt.ylabel('GDP per capita')
plt.title('GDP per Capita Over Time')
plt.legend()
plt.show()


# - The line chart shows the trend of GDP per capita over time for selected countries. It highlights differences in economic growth, with some countries showing steady increases while others grow at a slower rate. This visualization helps compare economic performance across countries over time.

# - I will use the above visualization to relate to emissions using the CO2 emissions series from the dataset on the same countries.

# In[456]:


wdi_cleaned['Series Name'].unique()


# In[457]:


# Getting the emissions data to check against the countries above.
emissions_data = wdi_cleaned[
    wdi_cleaned['Series Name'] == 'Carbon dioxide (CO2) emissions excluding LULUCF per capita (t CO2e/capita)'
]


# In[458]:


# Extract the emissions data for the selected countries to compare with the GDP data.
emissions_data = emissions_data[
    emissions_data['country'].isin(countries)
]

emissions_data.head()


# In[459]:


plt.figure(figsize=(10,6))

for country in countries:
    subset = emissions_data[emissions_data['country'] == country]
    plt.plot(subset['year'], subset['series_value'], label=country)

plt.xlabel('Year')
plt.ylabel('CO2 Emissions per Capita')
plt.title('CO2 Emissions Over Time')
plt.legend()
plt.show()


# - The line chart shows how CO₂ emissions per capita changed over time for the United States, China, and India. The United States shows a declining trend in emissions despite remaining the highest among the three. In contrast, China exhibits a significant increase in emissions, particularly after 2000, reflecting rapid industrialization and economic growth. India shows a gradual increase in emissions but remains relatively low compared to the other countries. Overall, the visualization highlights how economic development stages influence environmental impact.

# ##### The World Bank dataset originally stored multiple indicators in a single column, with their names in the Series Name column. A pivot operation was applied to convert each unique value in Series Name into its own column using country and year as keys. This resulted in a structured dataset where each indicator became a separate feature suitable for machine learning.

# In[460]:


wdi_pivot = wdi_cleaned.pivot_table(
    index=['country', 'year'],
    columns='Series Name',
    values='series_value'
).reset_index()


# In[461]:


# Check the data in the wdi_pivot DataFrame after pivoting.
wdi_pivot.head()


# In[462]:


# Rename the columns in the wdi_pivot DataFrame for meaningful names.
wdi_pivot = wdi_pivot.rename(columns={
    'Carbon dioxide (CO2) emissions excluding LULUCF per capita (t CO2e/capita)': 'co2_per_capita_wdi',
    'Energy use (kg of oil equivalent per capita)': 'energy_use',
    'GDP per capita (current US$)': 'gdp_per_capita',
    'Industry (including construction), value added per worker (constant 2015 US$)': 'industry_value',
    'Methane (CH4) emissions (total) excluding LULUCF (Mt CO2e)': 'methane_wdi',
    'Population density (people per sq. km of land area)': 'population_density',
    'Renewable energy consumption (% of total final energy consumption)': 'renewable_energy'
})
wdi_pivot.head()


# In[463]:


wdi_pivot.info()


# - Cleanup data in the Methane dataframe

# In[469]:


Methane_final_df.head()


# In[470]:


# Methane_final_df has the years in range as visible in abouve head commmand. Splitting this data into multiple rows based on the year column 
# to make it easier to merge with other dataframs

# Create an empty list to store new rows
expanded_rows = []

# Loop through each row
for _, row in Methane_final_df.iterrows():

    year_value = row['baseYear']

    # Check if it's a range like '2019-2021'
    if isinstance(year_value, str) and '-' in year_value:
        start, end = year_value.split('-')
        start = int(start)
        end = int(end)

        # Create a row for each year in the range
        for y in range(start, end + 1):
            new_row = row.copy()
            new_row['year'] = y
            expanded_rows.append(new_row)

    else:
        # If it's a single year
        new_row = row.copy()
        new_row['year'] = int(year_value)
        expanded_rows.append(new_row)

# Convert list back to DataFrame
Methane_final_df = pd.DataFrame(expanded_rows)


# In[471]:


#Check the new DataFrame after expanding the year ranges.
Methane_final_df.head()


# In[472]:


# Rename the columns in Methane_final_df to standardize the data sets
Methane_final_df = Methane_final_df.rename(columns={
    'country_name': 'country',
    'year': 'year'
})

print(Methane_final_df.columns)


# In[473]:


# Checking the data types of the columns in the Carbon_emissions_df DataFrame.
print(Carbon_emissions_df.dtypes)


# In[474]:


# Rename the carbon_emissions_df DataFrame columns to standardize the data sets.
Carbon_emissions_df = Carbon_emissions_df.rename(columns={
    'Country': 'country',
    'Kilotons of Co2': 'co2',
    'Metric Tons Per Capita': 'co2_per_capita'
})


# In[475]:


print(Carbon_emissions_df.columns)


# In[476]:


Carbon_emissions_df.info()


# In[477]:


# Add a new column year to the carbon_emissions_df DataFrame by extracting the year 
# from the Date column. This will be used to join to the other data frames.

#Convert the Date column to date format.
Carbon_emissions_df['Date'] = pd.to_datetime(Carbon_emissions_df['Date'], errors='coerce')

#Extract the year from the Date column.
Carbon_emissions_df['year'] = Carbon_emissions_df['Date'].dt.year

Carbon_emissions_df[['Date', 'year']].head()


# In[478]:


Carbon_emissions_per_capita_df = Carbon_emissions_per_capita_df.rename(columns={
    'Entity': 'country',
    'Year': 'year',
    'Annual CO₂ emissions (per capita)': 'co2_per_capita'
})


# In[479]:


print(Carbon_emissions_per_capita_df.columns)


# In[480]:


Carbon_emissions_per_capita_df.head()


# In[481]:


# Checking the data types of the columns in the Carbon_emissions_per_capita_df DataFrame.
print(Carbon_emissions_per_capita_df.dtypes)


# In[482]:


# Change the data for country to be uniform accross all the datasets.
# This will help us join the datasets together.

Methane_final_df['country'] = Methane_final_df['country'].str.lower().str.strip()
Carbon_emissions_df['country'] = Carbon_emissions_df['country'].str.lower().str.strip()
Carbon_emissions_per_capita_df['country'] = Carbon_emissions_per_capita_df['country'].str.lower().str.strip()

database_result['country'] = database_result['country'].str.lower().str.strip()
wdi_cleaned['country'] = wdi_cleaned['country'].str.lower().str.strip()


# ### 1. Machine Learning Plan

# #### What type of machine learning model are you planning to use?

# ##### I'm planning to use supervised machine learning models like regression models to predict carbon emissions. My target variable would be CO2 emissions. I will use models such as simple linear regression model, multiple linear regression model, and polinomial regression model to analyze relationships between variables like GDP, population, and emissions.

# #### What are the challenges have you identified/are you anticipating in building your machine learning model?

# ##### Some challenges I anticipate include handling missing values in the datasets, as climate data is often incomplete for certain countries and years. Another challenge is merging multiple datasets, since they may use different country names or formats. I also expect issues with feature selection, as not all variables will be equally useful for predicting CO₂ emissions. Additionally, there is a risk of overfitting, especially when using polynomial regression models.

# #### How are you planning to address these challenges?

# ##### To address these challenges, I will handle missing values by either filling them with appropriate statistics or removing incomplete records when necessary. I will standardize country names and formats to ensure accurate merging of datasets. For feature selection, I will choose relevant variables based on their correlation with CO₂ emissions. Also, I will evaluate model performance using a train-test split and RMSE to reduce overfitting and ensure the model generalizes well.

# #### Combining the datasets to create a final dataframe on which I will apply the machine learning regression models.

# In[483]:


print(database_result.columns)
print(wdi_cleaned.columns)
print(Carbon_emissions_df.columns)
print(Carbon_emissions_per_capita_df.columns)


# In[484]:


# Combine the database result DataFrame with methane dataframe.
final_df = database_result.merge(
    wdi_pivot,
    on=['country', 'year'],
    how='left'
)

# Merge the carbon emissions DataFrame with the final_df DataFrame.
final_df = final_df.merge(
    Carbon_emissions_df,
    on=['country', 'year'],
    how='left'
)

# Merge the carbon emissions per capita DataFrame with the final_df DataFrame.
final_df = final_df.merge(
    Carbon_emissions_per_capita_df,
    on=['country', 'year'],
    how='left'
)

# final_df = final_df.merge(
#     Methane_final_df,
#     on=['country', 'year'],
#     how='left'
# )


# - Removing the methan_final_df data set because, the data set from world bank data and our world in data has already this feature

# ##### Perform Exploratory Data analysis on the final_df dataframe

# In[485]:


# Check the info for the final Dataframe.
final_df.info()


# In[364]:


# Remove unnecessary columns from the final_df DataFrame that are not needed for our analysis.

final_df = final_df.drop(columns=[
    'co2_y',                  # duplicate source
    'co2_per_capita_y',       # too many missing values
    'Date',                   # not needed (year already exists)
    'methane_wdi',            # duplicate source
    'co2_per_capita_x',       # duplicate source
    'country'                 # Not needed for Machine Learning.
])


# In[365]:


final_df.describe()


# In[ ]:


# Check for missing values in the final DataFrame.
final_df.isnull().sum()


# - The dataset contains missing values mainly in the World Bank features, while core variables like CO₂, methane, and population are complete. These missing values occur due to limited data availability across countries and years and will be handled using imputation during preprocessing.

# In[367]:


# Rename the columns in the final_df DataFrame to standardize the data sets.
final_df.rename(columns={
    'co2_x': 'co2'
}, inplace=True)

final_df.info()


# In[369]:


# Check the correlation between the numerical columns in the final DataFrame to understand the relationships
#  between different variables.
final_df.corr(numeric_only=True)


# In[372]:


# Prepare the data for machine learning by splitting the final DataFrame into features (X) and target variable (y),
X = final_df.drop(columns=['co2'])
y = final_df['co2']

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


# In[373]:


# Seperate numerical and categorical columns.
num_cols = X_train.select_dtypes(include=['int64', 'float64']).columns
cat_cols = X_train.select_dtypes(include=['object']).columns

print("Numeric columns:", num_cols)
print("Categorical columns:", cat_cols)


# In[377]:


# Impute missing values in the numerical columns with the median and scale the features using StandardScaler. 
# This will help to handle any missing values and ensure that the features are on a similar scale for machine learning algorithms.
num_pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])


# In[379]:


# Impute missing values in the categorical columns with the most frequent value 
# and encode the categorical variables using OneHotEncoder.
cat_pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])


# In[381]:


# Combine the numerical and categorical pipelines into a ColumnTransformer 
# to apply the appropriate transformations to each type of data.
preprocessor = ColumnTransformer([
    ('num', num_pipeline, num_cols),
    ('cat', cat_pipeline, cat_cols)
])


# In[ ]:


# Fit and transform the training data.
X_train_prepared = preprocessor.fit_transform(X_train)
X_test_prepared = preprocessor.transform(X_test)


# In[383]:


print(X_train_prepared.shape)
print(X_test_prepared.shape)


# - The dataset was split into training and testing sets using an 80-20 ratio. The training set contains 23,272 records, while the test set contains 5,818 records. Each dataset includes 14 features used to predict CO₂ emissions, ensuring sufficient data for model training and evaluation.

# In[ ]:


# Train a linear regression model.
lin_reg = LinearRegression()
lin_reg.fit(X_train_prepared, y_train)


# In[385]:


# Train a polynomial regression model.
poly_model = Pipeline([
    ('poly_features', PolynomialFeatures(degree=2, include_bias=False)),
    ('lin_reg', LinearRegression())
])

poly_model.fit(X_train_prepared, y_train)


# In[390]:


lin_train_preds = lin_reg.predict(X_train_prepared)
lin_train_rmse = np.sqrt(mean_squared_error(y_train, lin_train_preds))
print("Linear Regression RMSE (train):", lin_train_rmse)

lin_test_preds = lin_reg.predict(X_test_prepared)
lin_test_rmse = np.sqrt(mean_squared_error(y_test, lin_test_preds))
print("Linear Regression RMSE (test):", lin_test_rmse)


# In[393]:


poly_train_preds = poly_model.predict(X_train_prepared)
poly_train_rmse = np.sqrt(mean_squared_error(y_train, poly_train_preds))

print("Polynomial Regression RMSE (train):", poly_train_rmse)

poly_test_preds = poly_model.predict(X_test_prepared)
poly_test_rmse = np.sqrt(mean_squared_error(y_test, poly_test_preds))

print("Polynomial Regression RMSE (test):", poly_test_rmse)


# - The Linear Regression and Polynomial Regression models were evaluated using RMSE on both training and test datasets. The Polynomial Regression model produced lower RMSE values on both the training set (613.69) and the test set (634.10) compared to the Linear Regression model. This indicates that the Polynomial model was able to better capture the underlying patterns in the data. Since the test RMSE is also lower, there is no significant overfitting observed. Therefore, the Polynomial Regression model performed better and was selected as the final model.

# ## Resources and References
# *What resources and references have you used for this project?*
# 📝 <!-- Answer Below -->
# 
# I used the lecture videos predominantly to help me with this project and I also used the course notes.
# 
# My datasets were imported from kaggle: https://www.kaggle.com
# 
# I also got data from our world in data website: 
# https://ourworldindata.org/co2-and-greenhouse-gas-emissions?insight=many-countries-have-reduced-their-co2-emissions#key-insights
# 
# I imported another data set during checkpoint 3 from the world bank development indicator website
# https://databank.worldbank.org/source/world-development-indicators#

# In[184]:


# ⚠️ Make sure you run this cell at the end of your notebook before every submission!
get_ipython().system('jupyter nbconvert --to python source.ipynb')

