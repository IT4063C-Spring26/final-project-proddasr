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

# Global emissions - https://www.kaggle.com/datasets/ashishraut64/global-methane-emissions
# CO2 emissions - https://www.kaggle.com/datasets/ulrikthygepedersen/co2-emissions-by-country
# Carbon (CO2) emissions - https://www.kaggle.com/datasets/ravindrasinghrana/carbon-co2-emissions

# ## Approach and Analysis
# *What is your approach to answering your project question?*
# *How will you use the identified data to answer your project question?*
# 📝 <!-- Start Discussing the project here; you can add as many code cells as you need -->

# In[ ]:


# Start your code here


# ### I will use line charts to track the amount of emissions.

# ### Are there any duplicate values? how are you going to deal with them?

# In[6]:


import pandas as pd
file_df = pd.read_table('Methane_final.csv')


# ## Resources and References
# *What resources and references have you used for this project?*
# 📝 <!-- Answer Below -->

# In[1]:


# ⚠️ Make sure you run this cell at the end of your notebook before every submission!
get_ipython().system('jupyter nbconvert --to python source.ipynb')

