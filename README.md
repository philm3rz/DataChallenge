# DataChallenge
This repository hosts the code for the 2020 DBL Data Challenge. Analyzing 35GB of Tweets related to 13 airlines, our team will find general trends, develop suggestions on how to gain a better public image, and what a specific airline can do in order to satisfy their clients.  

To recreate our work, files should be run as follows:
- first_pass_final.js, conv_tree.js, conv_tree_import_part.js and whose_import.js clean the redundant and missing data that can be fed to mongodb
- data_challenge.ipynb is the first python notebook with initial overall findings and creates a later used dictionary in save.p
- Statistics_Paula.ipynb updates the dictionary and saves it to save2.p
- sentiment-analysis-j.ipynb is our sentiment analysis notebook 
- Word_Frequency_Analysis.ipynb 
- first_pass_final.js was updated and it contains code for restructuring the data to sql format 
- Helperlite.ipynb contains helper functions for working with sql
- BotResearch.ipynb is an independant notebook which was not included in the report as the research wasn't successful
- convos_for_sa.ipynb builds the structure for conversation sentiment analysis and researches response bias
- Convos_Sentiment.ipynb performs sentiment analysis on that structure
- Time-series analysis.ipynb 
- topic modelling.ipynb 




