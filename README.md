# Data-Engineering-Project

In this project, we are exploring a dataset that contains records of participants in the summer and winter olympics starting from the olympics held at Athens 1896 to Rio2016. This contained information for each participant are: uniqueID, Name, Age, Height, Weight, Sex, Team, NOC (National Olympic committee), Year, Season (Summer or Winter), Games(Year and Season), City, Sport, Event and Medal. The number of records in this dataset is 271116

The goal of this project is to provide insight about how the Olympics have changed over time, such as how each gender's participation and performance have changed over time, as well as questions regarding different seasons,nations, sports, and events.

## Milestone1

In this milestone, we explored the dataset by observing the number of unique values in each column, viewing some records and viewing a basic description of the numerical values in the dataset. We also handled the missing values and outliers using some methods that will be explained below, and we did some visualization to have a better idea about the records in the dataset.

### Outliers

In order to handle the outliers, we grouped each sport and gender together in order to be sure that the outliers are handled properly; as the majority of participants in weightlifting and basketball events usually have a much larger weight and/or height when compared to the rest of the dataset so they can be considered an outlier if we chose to handle the outliers by using the whole dataset, so we wanted to make sure that we only handle the outliers if they are actually outliers within their sport and gender by replacing them with the mean of their sport and gender, and this was done in order to handle the outliers in the age, weight and height columns using the zscore.

### Missing Values

In order to handle the missing values, we grouped each sport and gender together in order to be sure that the missing values are handled properly, which is similar to what we did in the outliers for the same reasons mentioned in the outliers part. However, since some sports and genders didn't include any records about the weight and height of its participants, we replaced these missing values in these sports and gender with the whole dataset's mean.

### Data Exploration Questions

In this milestone, we did some visualizations that highlighted the top countries and athletes using the number of medals they won, the highest sports in the average age, weight and height of its participants and the difference in these metrics between each gender, the distribution of medalists according to their ages, heights and weights, and the variation of age, weights and heights accross different sports, years and countries.

## Milestone2
In this milestone, we integrated multiple datasets into our olympics dataset in order to be able to fully answer all our research questions, and we created two features for the same purpose.

### Data Integration
#### NOC regions
we used this dataset in order to get a regions column using the noc column present in our original olympics dataset, which is similar to the team column but without the inconsistencies that are in the team column. We used this newly added column in order to analyze the performance of each team.

#### Hosting Country
We used this dataset in order to map each city to its country in order to be able to identify the name of the hosting country so that we are able to know where each olympics were held. It was important that the name of the Hosting Country was the exact same name present in the NOC regions dataset in order to be able to know if team/region X was the same/different from the Hosting country.

#### continent
We used this dataset in order to map each region in our dataset to its corresponding continent, and this helped us in analyzing the performance of each continent as a whole.

#### Hosting Continant
We used this dataset in order to map each city to its continent which helped us in visualizing the performance of each continent when they were playing in the same/different continent.

### Feature Engineering
#### BMI and BMI_group
The BMI feature was created using the formula BMI=(Weight(kg))/(Height(M)^2) which is used to categorize each individual as underweight, normal weight and overweight. After calculating the BMI, we use these values in order to divide athletes to 3 different groups;  2 indicates normal weight, 1 indicates overweight or underweight, 0 indicates extremely underweight or obese, where the highest value in the bmi_group is better.

#### Participation Number
The Participation Number feature indicates the number of olympics this athlete has participated in so far until the year of the row. Its value starts from 1 when s/he participates for the first time in the olympics and it increases whenever s/he participates in another following olympics.

## Milestone3
In this milestone, we utilized Airflow to create a pipeline for the data we have.

### Data Pipeline
In this part, we created a dag which had multiple python operators; one for extracting the data, one for cleaning the data (handling missing data and outliers), one for integrating multiple datasets into our dataset, one for adding new features to the dataset, and one for saving our new dataset. We had to do these tasks in this order, so we set up some dependancies in order to be sure that these operators are executed in a specific order.

### Tweets Sentiment Analysis
Using the Twitter API library called Tweepy, we were able to fetch the tweets using the API.search() function and giving it the parameter of the country name in order to search for tweets that include the country and using count=20 in order to fetch 20 tweets. We then applied the TextBlob library for sentiment analysis using the Naive Bayes Analyzer which we applied on the text of the tweets as the analysis technique, and it returned a value of either 'pos' or 'neg', which stands for positive and negative, respectively. We then appended the classifications to a list which would end up being of length 20. We then count the number of 'neg' occurrences in the array. If it is bigger than 10, the majority is negtive hence the average will be 'negative'. If it is not, we decide that the average is 'positive'.
