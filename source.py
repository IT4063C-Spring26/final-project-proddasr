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

# Data set Files:
# 
# Database: https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv
# 
# Carbon (CO2) emissions - https://www.kaggle.com/datasets/ravindrasinghrana/carbon-co2-emissions
# 
# Greenhouse Gas Emissions - https://www.kaggle.com/datasets/willianoliveiragibin/greenhouse-gas-emissions
# 
# Global emissions - https://www.kaggle.com/datasets/ashishraut64/global-methane-emissions
# 
# 
# I will use the dataset from Our World in Data, which has a complete global data on emissions, population, and other climate-related indicators. I will store and structure this dataset in a relational database using SQLite.
# 
# I will merge the kaggle datasets using common variables like country and year as both datasets include emissions data for different countries for various years.
# 
# I will create the following tables countries, emissions, and population from Ourworld dataset and I will create a unique identifier country_id accross these tables to join them. I will then join the dataset from this to kaggle data sets using country name and year

# ## Approach and Analysis
# *What is your approach to answering your project question?*
# *How will you use the identified data to answer your project question?*
# 📝 <!-- Start Discussing the project here; you can add as many code cells as you need -->
# 
# My approach is to combine emissions data from multiple datasets and analyze trends over time. I will clean and merge the datasets using common variables such as country and year. The data will be stored and queried using a SQLite database. I will then create visualizations, such as line charts, to observe how emissions change over time and compare patterns across countries. This analysis will help determine how emissions relate to climate change trends.

# In[9]:


# Import necessary libraries for data manipulation and visualization.
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3


# In[10]:


# Import the data from Our World in Data and store it in a DataFrame.

url = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"
df = pd.read_csv(url)

df.sample(5)


# In[11]:


# Create a new DataFrame with the relevant columns for distinct countries and assign a unique country_id to each country.
countries = df[["country"]].drop_duplicates().reset_index(drop=True)

# Assign a unique country_id to each country using the index of the DataFrame. Adding 1 to start the country_id from 1 instead of 0.
countries["country_id"] = countries.index + 1

countries.sample(10)


# In[39]:


# Merge the country_id back to the main DataFrame

df = df.merge(countries, on="country")
df.sample(10)


# In[13]:


# Create a new DataFrame with the relevant columns for emissions.
population = df[["country_id", "year", "population"]]
population.head()


# In[14]:


# Create a new DataFrame with the relevant columns for emissions.
emissions = df[["country_id", "year", "co2", "methane"]]

emissions.sample(5)


# In[15]:


# Store the DataFrames in a SQLite database.
conn = sqlite3.connect("climate.db")

countries.to_sql("countries", conn, if_exists="replace", index=False)
emissions.to_sql("emissions", conn, if_exists="replace", index=False)
population.to_sql("population", conn, if_exists="replace", index=False)


# In[132]:


# Write a SQL query to join the three tables and retrieve the country name, year, CO2 emissions, methane emissions, and population.
query = """
SELECT c.country, e.year, e.co2, e.methane, p.population
FROM emissions e
JOIN countries c ON e.country_id = c.country_id
JOIN population p ON e.country_id = p.country_id AND e.year = p.year
"""

database_result = pd.read_sql(query, conn)
database_result.sample(5)


# In[165]:


# Import data from csv files imported from Kaggle and store it in DataFrames.
Methane_final_df = pd.read_table('Methane_final.csv', sep=',')
# Drop the 'Unnamed: 0' column from the Methane_final_df DataFrame, which is a unnecessary index column that was created during the CSV export process.
Methane_final_df = Methane_final_df.drop(columns=['Unnamed: 0'])

Carbon_emissions_df = pd.read_table('Carbon_(CO2)_Emissions_by_Country.csv', sep=',')

Carbon_emissions_per_capita_df = pd.read_table('co-emissions-per-capita new.csv', sep=',')


# ## 1. Exploratory Data Analysis (EDA)

# In[45]:


# Perform basic data analysis on the database_result DataFrame to understand the structure and summary statistics of the data.
database_result.info()
database_result.describe()


# ##### - My dataset from the database has yearly data for different countries showing the emission details like CO2 and methane and it also lists population.
# ##### - CO2, methane, and population have a lot of missing data.

# In[48]:


# Checks for missing values in the database_result DataFrame.
database_result.isnull().sum()


# In[66]:


# Checking the data types of the columns in the database_result DataFrame.
print(database_result.dtypes)


# In[49]:


# Check for duplicates in the database_result DataFrame.
database_result.duplicated().sum()


# In[51]:


# Perform basic data analysis on the Methane_final_df DataFrame to understand the structure and summary statistics of the data.
Methane_final_df.info()
Methane_final_df.describe()


# ##### - My dataset for methane dataset shows methane emission details including the type segment etc. for each country per year
# 
# ##### - This dataset doesn't have any missing values.

# In[60]:


# Checking for null values in the Methane_final_df DataFrame.
Methane_final_df.isnull().sum()


# In[56]:


# Check for duplicates in the Methane_final_df DataFrame.
Methane_final_df.duplicated().sum()


# In[63]:


# Checking the data types of the columns in the Methane_final_df DataFrame.
print(Methane_final_df.dtypes)


# ##### - The dataset for carbon emissions dataframe contains C02 emission details for each country based on a date.
# ##### - There is no null values in this dataset.
# ##### - There is no year field so, I need to a create a year field from the date to be able to join to the other datasets.

# In[53]:


Carbon_emissions_df.info()
Carbon_emissions_df.describe()


# In[ ]:


# Checked the data from the carbon emissions dataset.
Carbon_emissions_df.head(5)


# In[61]:


# Checking for null values in the Carbon_emissions_df DataFrame.
Carbon_emissions_df.isnull().sum()


# In[57]:


# Check for duplicates in the Carbon_emissions_df DataFrame.
Carbon_emissions_df.duplicated().sum()


# In[64]:


# Checking the data types of the columns in the Carbon_emissions_df DataFrame.
print(Carbon_emissions_df.dtypes)


# ##### - The carbon emissions per capita has data showing annual CO2 emissions per capita. For each entity(country) per year.
# ##### - There are no null values for this dataset.

# In[55]:


Carbon_emissions_per_capita_df.info()
Carbon_emissions_per_capita_df.describe()


# In[70]:


# Check the data from the carbon emissions per capita dataset.
Carbon_emissions_per_capita_df.sample(5)


# In[62]:


# Checking for null values in the Carbon_emissions_per_capita_df DataFrame.
Carbon_emissions_per_capita_df.isnull().sum()


# In[58]:


# Check for duplicates in the Carbon_emissions_per_capita_df DataFrame.
Carbon_emissions_per_capita_df.duplicated().sum()


# In[65]:


# Checking the data types of the columns in the Carbon_emissions_per_capita_df DataFrame.
print(Carbon_emissions_per_capita_df.dtypes)


# In[77]:


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

# In[78]:


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

# In[ ]:


# Create a bar plot to visualize the emissions by type.

region_emissions = Methane_final_df.groupby('type')['emissions'].mean()

plt.bar(region_emissions.index, region_emissions.values)

plt.title("Emissions by Type")
plt.xlabel("Type")
plt.ylabel("Emissions")
plt.xticks(rotation=45)

plt.show()


# ##### - The above bar plot shows a different type of methane emissions.
# ##### - Based on this graph I can tell that agriculture causes higher emissions than the rest.

# In[92]:


# Create a pie chart to visualize the share of CO2 emissions by country for the top 10 emitting countries.
top_co2 = Carbon_emissions_df.groupby('Country')['Kilotons of Co2'].sum().nlargest(10)

top_co2.plot(kind='pie', autopct='%1.1f%%')
plt.title("Top 10 CO2 Emissions Share by Country")
plt.show()


# ##### - The above pie chart shows emissions of the top 10 countries. This graph tells me that majority of the emsission are from these 10 countries.

# In[94]:


Carbon_emissions_per_capita_df.sample(10)


# In[ ]:


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

# ##### Clean up the data in database_result dataframe

# In[122]:


#Check missing values in the database_result DataFrame.
database_result.isnull().sum()


# In[ ]:


# Drop rows where CO2 is missing, because CO2 is an essential data point for our analysis.
database_result = database_result.dropna(subset=['co2'])

# Fill population with median
database_result['population'] = database_result['population'].fillna(database_result['population'].median())

# Fill methane with median.
database_result['methane'] = database_result['methane'].fillna(database_result['methane'].median())


# In[127]:


# Check again
database_result.isnull().sum()


# In[ ]:





# In[ ]:


# Dropping missing values from the result DataFrame.
result.dropna(inplace=True)


# In[ ]:


# Checking for null values after dropping them.
result.isnull().sum()


# In[ ]:


# Checking the data types of the columns in the Carbon_emissions_df DataFrame.
print(Carbon_emissions_df.dtypes)


# In[ ]:


# Checking the data types of the columns in the Carbon_emissions_per_capita_df DataFrame.
print(Carbon_emissions_per_capita_df.dtypes)


# In[ ]:


# Checking the data types of the columns in the result DataFrame.
print(result.dtypes)


# ##### Check for any outliers in the database_result dataframe

# In[137]:


# Create a box plot to visualize the distribution of CO2 emissions in the result DataFrame.
sns.boxplot(x=database_result['co2'])
plt.show()


# In[ ]:


# Remove outliers from the result DataFrame based on the CO2 emissions column.
# Calculate the 99th percentile of the CO2 emissions to identify outliers and filter out 
# those values from the database_result DataFrame.
database_result = database_result[database_result['co2'] < database_result['co2'].quantile(0.99)]


# In[155]:


Methane_final_df.head()


# In[167]:


# Rename the columns in Methane_final_df to standardize the data sets
Methane_final_df = Methane_final_df.rename(columns={
    'country_name': 'country',
    'year': 'year'
})

print(Methane_final_df.columns)


# In[169]:


# Rename the carbon_emissions_df DataFrame columns to standardize the data sets.
Carbon_emissions_df = Carbon_emissions_df.rename(columns={
    'Country': 'country',
    'Kilotons of Co2': 'co2',
    'Metric Tons Per Capita': 'co2_per_capita'
})


# In[170]:


print(Carbon_emissions_df.columns)


# In[180]:


Carbon_emissions_df.info()


# In[183]:


# Add a new column year to the carbon_emissions_df DataFrame by extracting the year 
# from the Date column. This will be used to join to the other data frames.

#Convert the Date column to date format.
Carbon_emissions_df['Date'] = pd.to_datetime(Carbon_emissions_df['Date'], errors='coerce')

#Extract the year from the Date column.
Carbon_emissions_df['year'] = Carbon_emissions_df['Date'].dt.year

Carbon_emissions_df[['Date', 'year']].head()


# In[172]:


Carbon_emissions_per_capita_df = Carbon_emissions_per_capita_df.rename(columns={
    'Entity': 'country',
    'Year': 'year',
    'Annual CO₂ emissions (per capita)': 'co2_per_capita'
})


# In[173]:


print(Carbon_emissions_per_capita_df.columns)


# In[ ]:


# Change the data for country to be uniform accross all the datasets.
# This will help us join the datasets together.

Methane_final_df['country'] = Methane_final_df['country'].str.lower().str.strip()
Carbon_emissions_df['Country'] = Carbon_emissions_df['Country'].str.lower().str.strip()
Carbon_emissions_per_capita_df['Entity'] = Carbon_emissions_per_capita_df['Entity'].str.lower().str.strip()

database_result['country'] = database_result['country'].str.lower().str.strip()


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

# In[1]:


# ⚠️ Make sure you run this cell at the end of your notebook before every submission!
get_ipython().system('jupyter nbconvert --to python source.ipynb')

