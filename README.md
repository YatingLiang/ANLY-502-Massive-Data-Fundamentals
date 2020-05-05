# **Project Information**


Course: ANLY 502 Massive Data Fundamentals Project 

Group Name: Avada Kedavra!

Contributors: Jiahui Chen, Yating Liang, Xueyan Liu, Xinyao Mo

Code files：


 - [`Data_Cleaning.ipynb`](https://github.com/YatingLiang/ANLY-502-Massive-Data-Fundamentals/blob/master/Data_Cleaning.ipynb): This file contains the data cleaning process for year 2019 weather data.
 
  - [`1950-1990stats.ipynb`](https://github.com/YatingLiang/ANLY-502-Massive-Data-Fundamentals/blob/master/1950-1990stats.ipynb): This file contains the trend and statistical analysis for 1950 to 1990 data (every five years). It measures the Average and the standard deviation of air temperature, visibility, and precipitation per hour.
  
  - [`AllYears_Correlation_Heatmap.ipynb`](https://github.com/YatingLiang/ANLY-502-Massive-Data-Fundamentals/blob/master/AllYears_Correlation_Heatmap.ipynb): This file contains the data cleaning process for 1900-2020 data of sampled stations and some visualizations such as correlation heatmap of all features.
  
  - [`EDA_Charts_Visual.ipynb`](https://github.com/YatingLiang/ANLY-502-Massive-Data-Fundamentals/blob/master/EDA_Charts_Visual.ipynb): This file contains the chart visualizations for 2019 data, such as pie charts, bar charts, line charts, and boxplot of some features.
  
  - [`EDA_Map_Visual.ipynb`](https://github.com/YatingLiang/ANLY-502-Massive-Data-Fundamentals/blob/master/EDA_Charts_Visual.ipynb):  This file contains the world map visualizations for 2019 data, which is also color-coded.
  
   - [`model_glr_pca.ipynb`](https://github.com/YatingLiang/ANLY-502-Massive-Data-Fundamentals/blob/master/model_glr_pca.ipynb): This file contains some model fitting and the results, such as the Generalized Linear Regression and Principal Component Analysis for 2019 data.
   
  - [`model_ridge_lasso_dt_gbt.ipynb`](https://github.com/YatingLiang/ANLY-502-Massive-Data-Fundamentals/blob/master/model_ridge_lasso_dt_gbt.ipynb): This file contains some model fitting and the results, such as the Ridge Regression, Lasso Regression, Decision Tree, and Gradient Boosting Tree for 2019 data.


# **Dataset**

**NOAA Integrated Surface Database (ISD):** https://registry.opendata.aws/noaa-isd/ 

**Amazon Resource Name (ARN):** `arn:aws:s3:::noaa-global-hourly-pds` 

**Description:** The Integrated Surface Database (ISD) consists of global hourly and synoptic observations compiled from numerous sources into a gzipped fixed width format. The database includes over 35,000 stations worldwide from 1990 to 2019.（Total size:1.1 TB)



# **Executive Summary**

The science of weather forecasting attracts public attentions every single day. Since weather and the temperature could decide what people might want to wear for the day, it might also affect people's moods. If the tempperature is low tomorrow, people should be able to get prepared to wearing warmer clothes. Thus, this research project tends to build several predictive models in order to predict temperatures using other weather indicators. The models include Ridge Regression, Lasso Regression, Decision Tree, Gradient Boosting Tree, Generalized Linear Regression, and Principal Component Analysis with varying degrees of accuracy.

# **1.Introduction**

This project focuses on weather data analysis. The NOAA Integrated Surface Database is used for the analysis, which contains Worldwide stations' hourly weather reports from 1900-2020. Some useful visualizations, such as geographical patterns, maps, and line charts, were drawn to explore the pattern of the dataset and different machine learning models were used to do predictive analysis on the temperature in the project.

There were many challenges during the research process. The first challenges was data cleaning, which will be further explained in the method part. The second challenges is to process large-scale spatial data and convert to readable map. This can be tricky because spatial data needed to be handled using specific spatial packages in pyspark. Finally, the modeling results came well with good performances, which would be detailed explained in the Models Results part.



# **2.Method** 

## 2.1 Data Cleaning  
 
Overall the data has 1TB total size from 1900 to 2020 and consists of a million small csv files in different folders and these files have multiple formats. For this project, some samples from all year range and the whole data for 2019 were used.

The original data was very dirty and the cleaning process is essential in this project. One of the challenging problems for weather research is the size of the data. The data is gathered from all the weather stations all around the world. If it was manipulated in its full size, it would take a great amount of extra time and is also a waste of computation power. The data was stored in S3 bucket as csv format, and one csv per station, which means a million small csv files.  Also, some stations measure more weather features than the others, that being said, the column number of each csv file might be different from each other.  After investigating into the data, only the first 11 variables were kept to do the analysis.  Therefore, removing unnecessary variables or observations that have missing values and selecting some useful features is vital to continue the research in the next step.

The year 2019 weather data (Total 50.4GB) was loaded by spark read-option function. Most entries in the dataset are string type containing null values for certain observations. Thus, converting them to float type and integer type is necessary. From the  [FEDERAL CLIMATE COMPLEX DATA DOCUMENTATION FOR INTEGRATED SURFACE DATA](https://www.ncei.noaa.gov/data/global-hourly/doc/isd-format-document.pdf) ,  some missing values are represented as “9999” or “99999”. Thus, they were removed to ensure the integrity of the data for modeling. Therefore,  the measures implemented in the data cleaning process were dropping useless columns to increase computation efficiency, removing null values in the data, and transform the data types to an easier handle one. 

The data cleaning process starts with selecting useful variables (columns). A total of 11 most informative variables were kept in the data set.  They were `date`, `latitude`,`longitude`,  `elevation`, `dew`(dew point temperature), `vis`(visibility), `liquid`(liquid precipitation per hour), `wind`(wind speed), `cig`(ceiling height dimension), `slp` (sea level pressure) and `temp`(air temperature). And finally, the data of 2019 for each station was merged together into one big csv file to do further research on this year's weather specifically.
Subset Example:   
|    station|               date|latitude| longitude|elevation|wind|   cig|    vis| temp|  dew|   slp|liquid|liqhr|month|
|    ------|               ------|------| ------|------|------|   ------|    ------| ------|  ------|   ------|------|------|------|
|72475023176|2019-01-01T03:52:00|38.41667|-113.01667|   1536.2| 4.1| 396.0| 2012.0| -8.3|-10.6|1019.3|   0.0|  1.0|    1|
|72475023176|2019-01-01T04:52:00|38.41667|-113.01667|   1536.2| 5.7| 335.0| 1609.0| -7.8| -9.4|1019.2|   0.5|  1.0|    1|  

Another way to create a subset of all the years reports to see the trend is sampling through all the stations and extract all available years' report of these stations. Even if from the same station, the reports’ formats are different. In order to obtain more completed data, the data was loop and merged from these reports by columns. Thus, using this new dataset, it is able to show the trends of various weather indicators and correlation matrix between each variable.   
Subset Example:   
|    STATION|               DATE|  LATITUDE| LONGITUDE|ELEVATION|WND1|WND2|WND4|WND5| CIG1|CIG2|CIG3|  VIS1|VIS2|VIS4| TMP1|TMP2| DEW1|DEW2| SLP1|SLP2|AA11|AA12|AA13|AA14|AY11|AY12|AY13|AY14|GF11|GF12|GF13|GF14|GF15|GF16|GF17| GF18|GF19|GF110|GF111|GF112|GF113|MD11|MD12|MD13|MD14|MD15|MW11|MW12|GA11|GA12|GA13|GA14|GA15|GA16|AA21|AA22|AA23|AA24| MA11|MA12| MA13|MA14|AY21|AY22|AY23|AY24| OA1| UG2| WA1|AW11|OC1|AW12|KA11|KA12|KA13| AZ1|WND_ind|CIG_ind|VIS_ind|
|    ------|               ------|  ------| ------|------|------|------|------|------| ------|------|------|  ------|------|------| ------|------| ------|------| ------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------| ------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------|------| ------|------| ------|------|------|------|------|------| ------| ------| ------|------|------|------|------|------|------| ------|------|------|------|
|01001099999|2011-01-01T00:00:00|70.9333333|-8.6666667|      9.0| 340|   1|0110|   1|01200|   1|   9|005000|   1|   1|-0056|   1|-0098|null|10264|   1|  06|9999|   9|   9|   8|   1|  06|   1|  08|  99|   1|  08|   1|  08|   1|00800|   1|   99|    9|   99|    9|   2|   1| 017|   1|+999|  85|   1|null|null|null|null|null|null|null|null|null|null|99999|   9|10252|   1|   3|   1|  06|   1|null|null|null|   0|  0|null|null|null|null|null|    0.0|    0.0|    0.0|
|01001099999|2011-01-01T01:00:00|70.9333333|-8.6666667|      9.0| 330|   1|0100|   1|99999|   9|   9|999999|   9|   1|-0054|   9|-0095|null|10268|   1|null|null|null|null|null|null|null|null|null|null|null|null|null|null|null| null|null| null| null| null| null|   2|   1| 016|   1|+999|null|null|null|null|null|null|null|null|null|null|null|null|99999|   9|10256|   1|null|null|null|null|null|null|null|   0|  0|null|null|null|null|null|    0.0|    0.0|    0.0|
 
## 2.2 Visualizations    
One of our project goals is discovering the geographical pattern of weather data in 2019. The best way to reach the goal is visualizing the data by a world map. However, it is not realistic to handle more than ten thousands of stations' data in the common data frame. Thus we introduced the Geospark.   Geospark is a cluster computing system for processing large-scale spatial data and is used by Mobike company, supporting their Real-time locating system. It contains several modules: core(RDD), SQL/Dataframe, Vis, and Zeppelin. (Python only contain RDD, and SQL for now) In python, GeoSpark is an extension of pyspark functions. Instead of common RDD, Geospark core provides 5 special SpatialRDD: PointRDD, PolygonRDD, LineStringRDD, CircleRDD, RectangleRDD. These RDD will automatically convert data to GeoData which have 2 attributes: geometry:  shapely geometry representation (consists of latitude and longitude ) as shapely objects; and userDate : string representation of other attributes.  For visualizing the data, we also implemented the libraries: Geopandas, Matplotlib and Seaborn. The Geopandas can handle the geospatial data, and it combines the capabilities of pandas and shapely. The Matplotlib and Seaborn allow us to create multiple types of visualizations from the data frame. 


## 2.3 Models

**Data Preprocessing**

In this part, we use NOAA Dataset in 2019 for modeling. We group the data by “station” and get the average value of variables “latitude”, “longitude”, “elevation”, “wind”, “cig”, “dew”, “slp”, “liquid”, and“temp”. Meanwhile, we transfer all the variables into type “float” for modeling. For here, we set features as “liquid”, “latitude”, “dew”, “vis”, “longitude”, “wind”, “elevation”, “cig”, “slp”, and set target as “temp”, which means average temperature of each station.

**Hypothesis**

The features “liquid”, “latitude”, “dew”, “vis”, “longitude”, “wind”, “elevation”, “cig”, “slp” are influence factors of “temp”, and these features could lead to the relatively accurate predictions of target “temp”.

### 2.3.1 Ridge Regression

Ridge Regression is particularly useful to mitigate the problem of multicollinearity in linear regression, which commonly occurs in models with large numbers of parameters. In general, this method provides improved efficiency in parameter estimation problems in exchange for a tolerable amount of bias. With our dataset, there are nine predictors, which is a large number. Therefore, Ridge Regression has been selected to improve efficiency.

### 2.3.2 Lasso Regression

Lasso can shrink the coefficients and reduce the model complexity and multicollinearity as Ridge. Meanwhile, Lasso's regularization can lead to zero coefficients, and thus some of the features are completely neglected for the evaluation of output. Therefore, Lasso Regression could not only reduce the overfitting problem, but also help us in feature selection. It is also suitable for our dataset with many parameters.

### 2.3.3 Decision Tree

The Decision Tree is a flowchart-like structure in which each internal node represents a "test" on an attribute, each branch represents the outcome of the test, and each leaf node represents a class label. The paths from root to leaf represent classification rules. The number of features may bring trouble when interpreting the modeling results. Decision Trees are simple to understand and interpret. This model can identify the most effective predictors, represented by decision nodes. Therefore, this model can figure out what predictors make a big difference in average temperature, and provides an interpretable way to predict target “temp”.

### 2.3.4 Gradient Boosting Tree

Gradient boosting is a machine learning technique for regression and classification problems, which produces a prediction model in the form of an ensemble of weak prediction models. Its model can verify and improve the results of the Decision Tree.

### 2.3.5 Generalized Linear Regression

Contrasted with linear regression where the output is assumed to follow a Gaussian distribution, generalized linear models (GLMs) are specifications of linear models where the response variable follows some distribution from the exponential family of distributions. With our dataset, try the gamma kernel and discuss the possible difference.

### 2.3.6 Principal Component Analysis

Principal component analysis (PCA) is a statistical method to find a rotation such that the first coordinate has the largest variance possible, and each succeeding coordinate, in turn, has the largest variance possible. The columns of the rotation matrix are called principal components. PCA is used widely in dimensionality reduction. PCA can help to understand the variable importance of the indicators.




# **3.Results** 

## 3.1 Worldmap analysis  
The points on the world map show the locations of stations. (Figure 3.1.1 : shows the elevation of each station.) Obviously, the distribution of weather stations is not uniform. In the United States and Europe, the density of stations is significantly higher than in other places. It looks like that if a country is richer, it may build more stations and conduct more academic research. We also noticed that the most stations are built at north temperate zone, the area has four distinct seasons. 
> ![Figure.1](/image/worldmap-EVELVATION.png)  
> Figure 3.1.1         
----
*Temperature*  
We observed the temperature from two aspects: Temperature variation and average temperature. (Figure 3.1.2: shows average temperature of each station in 2019, the blue points are the 50 coldest stations,  and the red points are the hottest stations. Figure 3.1.3: shows the temperature variation of each station in 2019.) 
> ![Figure.2](/image/worldmap-TEMP-WITH%20HIGH%20LOW%20station.png)  
> Figure 3.1.2 
> ![Figure.3](/image/worldmap-TEMP-VAR.png)  
> Figure 3.1.3 
----
According to the colour distribution, we can see the area with the higher average temperature tend to have less variation. The hottest stations are concentrated in low latitudes, the coldest stations are in high latitudes. Furthermore, we also found something surprised us. By common sense, we made a hypothesis that the annual average temperature of the station is lower while the elevation of the station is higher. However, from the graph, the distribution of elevation data and the distribution of average temperature is not similar. elevation and temperature don’t have a strong correlation.   

*Other weather indicators:*    
The following six figures respectively are: average precipitation，average visibility, average wind speed, average sea level pressure, average dew and average height of the lowest layer of clouds (ceiling height dimension) of each station in 2019. These plots offer us plenty of information, for instance:  Rainfall is greater in Asia than other continents, and inland seems to have more rainfall than coastal; sea-level pressures vary widely in different regions of the world; the lower the longitude, the lower ceiling height dimension. Some climate indicators have similar color distributions, such as precipitation and sea level pressure; dew, and visibility; ceiling height dimension and temperature. These similarities represent the distinct climatic characteristics in different regions and the correlation between temperature and other weather indicators which help us select the variables for prediction.
> ![Figure.4](/image/worldmap-PRECIP.png)  
> Figure 3.1.4
> ![Figure.5](/image/worldmap-VIS.png)  
> Figure 3.1.5
> ![Figure.6](/image/worldmap-WIND.png)  
> Figure 3.1.6
> ![Figure.7](/image/worldmap-SLP.png)  
> Figure 3.1.7
> ![Figure.8](/image/worldmap-DEW.png)  
> Figure 3.1.8
> ![Figure.9](/image/worldmap-CIG.png)  
> Figure 3.1.9
----  

## 3.2 Weather indicators trends   

### 3.2.1 Monthly trend analysis for 2019

**Distribution of stations - 5 zones**
> ![Figure.0](/image/fivezonespiechart.png)  
> Figure 3.2.1.0
----

Stations(93.3% of all) are mostly within North Temperate Zone, which is between Arctic Circle and Tropic of Cancer.


**Monthly Average Temperature(Celsius)**

July has the highest average temperature and January has the lowest in 2019. The avergate air temperature is going up then down for the year which makes sense because most stations are in the North Temperate Zone.

> ![Figure.1](/image/monthavgtempline.png)  
> Figure 3.2.1.1

> ![Figure.2](/image/monthtempbox.png)  
> Figure 3.2.1.2
----


**Monthly Average Liquid Precipitation(millimeters/hour)**

March has the lowest Liquid Preciption and October has the highest Liquid Precipitation in 2019.

> ![Figure.3](/image/monliquidbar.png)  
> Figure 3.2.1.3
----

**Monthly Average Visibility(Meters)**

Febuary has the avergae lowest visibility in 2019. Boxplot shows lots of outliers.

> ![Figure.4](/image/monvisline.png)  
> Figure 3.2.1.4

> ![Figure.5](/image/monvisbox.png)  
> Figure 3.2.1.5


**Monthly Average Wind Speed(Meters/Sec)**

August has the lowest wind speed in 2019.

> ![Figure.6](/image/monwindbar.png)  
> Figure 3.2.1.6

> ![Figure.7](/image/monwindbox.png)  
> Figure 3.2.1.7
----

**Monthly Average Sea Level Pressure**

January has the highest Sea Level Pressure, and May has the lowest Sea Level Pressure in 2019.

> ![Figure.8](/image/monslpline.png)  
> Figure 3.2.1.8

### 3.2.2 Yearly trend analysis for 1950-1990

**Every 5 year Average Air Temperature(Celsius)**
> ![Figure.1](/image/5090avgtemp.png)  
> Figure 3.2.2.1

**Every 5 year Standard Deviation of Air Temperature(Celsius)**
> ![Figure.2](/image/5090stdtemp.png)  
> Figure 3.2.2.2

**Every 5 year Average Visibility Distance(Meters)**
> ![Figure.3](/image/5090avgvis.png)  
> Figure 3.2.2.3

**Every 5 year Standard Deviation of Visibility Distance(Meters)**
> ![Figure.4](/image/5090stdvis.png)  
> Figure 3.2.2.4

**Every 5 year Average Liquid Preciption Depth(Millimeters/hr)**
> ![Figure.5](/image/5090avgliquid.png)  
> Figure 3.2.2.5

**Every 5 year Standard Deviation of Liquid Preciption Depth(Millimeters/hr)**
> ![Figure.6](/image/5090stdliquid.png)  
> Figure 3.2.2.6
----

## 3.3 Variables selection   
For selecting variables for temperature prediction, we also introduced the heatmap of the correlation matrix (Figure 3.3.1). The heatmap illustrates the correlation between variables. Blue reflects the negative correlation while red reflects positive correlations. Darker colours indicate stronger correlations. According to the heatmap, we find the correlations between most the variables are weak.  Only a few of them holds a strong correlation which indicates that the low multicollinearity issue risk. Then we take look at the temperature’s correlation with other variables. By the sorted correlation coefficient, we selected wind speed, the height of the lowest layer of clouds, visibility, sea level pressure, precipitation and rainfall duration as our variables. (Figure 3.3.2 are the scatter plot of temperature with the other two variables.)
> ![Figure.10](/image/corr_full.png)  
> Figure 3.3.1   
> ![Figure.11](/image/SCA-TEMP-VS-DEW.png)  ![Figure.12](/image/SCA-TEMP-VS-ELE.png)  
> Figure 3.3.2      
----
  
## 3.4 Models Results

### 3.4.1 Performance Comparison 

Computing the r-squared and RMSE of each models and results are shown below. We can see that generally, all models achieve a r-squared of about 90% and RMSE of about 3. Among all Gradient Boosting Tree has the best performance with highest  r-squared and lowest RMSE. In addition, we could find that tree models (Decision Tree and Gradient Boosting Tree) perform better than linear models (Ridge, Lasso and Generalized linear model), this means that tree models are more suitable to the dataset for this project.


|                |R-Squared  |RMSE     |
|----------------|------|----------|
|Ridge Regression|0.883  |3.003   |
|Lasso Regression |0.867  |3.146   |
|Decision Tree |0.905  |2.996   |
|Gradient Boosting Tree |0.927  |2.620 |
|Generalized Linear Regression |0.891|2.987|

We can further examine the quality of models. Below shows two kinds of plot of each linear models: "Actual vs fitted value" and "Residuals vs fitted value". For the first kind, an idea model should have scatters lie close to the line y=x as many as possible. For the latter, a good linear model will usually have residuals distributed randomly around the residuals=0 line with no distinct outliers and no clear trends. The residuals should also be small for the whole range of fitted values. We can see that all of the three models meet with this condition and therefore hav good quality.

<img width="450" height="450" src="/image/p&a_ridge.PNG"/> <img width="450" height="450" src="/image/res_ridge.PNG"/>
<img width="450" height="450" src="/image/p&a_lasso.PNG"/> <img width="450" height="450" src="/image/res_lasso.PNG"/>
<img width="450" height="450" src="/image/p&a_glr.PNG"/> <img width="450" height="450" src="/image/res_glr.PNG"/>
> Figure 3.4.1
----

### 3.4.2 Linear Models Coefficients 

Three models used in this project are linear model: ridge, lasso and generalized linear model. Thus we can get coefficients of each variables of this three models. Details are shown in the table below.

Both the ridge regression and generalized linear model take all of the indicators into models, but in lasso regression model, only the coefficients of ‘latitude', 'dew', 'elevation', and 'cig' are non-zero. This means that in lasso regression only four variables are used and thus is more simplified. In addition, of all three models, the coefficients of ‘dew' are always the biggest, which means that it is quite important.

|Coefficient |Ridge Regression  |Lasso Regression  |Generalized Linear Regression |
|--------|------|------|------|
|liquid | -0.1455 |0   |-0.1109   |
|latitude|-0.0286  | -0.0081  |-0.0219   |
|dew| 0.8651 |0.8688   |0.9041   |
|vis|-4.6491e-06  | 0  |6.0804e-6  |
|longitude |0.0015  |0   |-0.0013   |
|wind|0.1010  |0   |0.1473   |
|elevation|0.0016  |6.6948e-4 |0.0017   |
|cig|0.0002  |1.4483e-4   |0.0002   |
|slp|-0.0481  |0   |-0.0382   |
|intercept| 52.8043 | 4.4456  |41.6677   |


### 3.4.3 PCA Results 


We set the number of PC as three. We can see which variables have the largest effects in each PC from the table below. The features which have the highest magnitude in the first PC have the highest effect. From the table, we observe that vis has the highest effect in the first PC while in the second PC cig is the most important. 


|   | PC1|PC2|PC3|
|--------|------|------|------|
|liquid	| -0.00001	|0.00002	|-0.00001	|
|latitude 	|0.00048	|0.00053	|-0.00074	|
|dew	|-0.00034	|-0.00008	|0.00543	|
|vis	|  0.93689	|0.34960	| 0.00391	|
|longitude	|0.00127	|-0.00117	|0.01488	|
|wind	| 0.00001	| 0.00005	|0.00003	|
|elevation	| 0.00633	| -0.00575	| -0.99984	|
|cig	| 0.34956	|-0.93688 	|0.00758	|
|slp 	|0.00007	| -0.00014	|-0.00165	|


The variance explained by each PC is shown below. The PCs are ranked based on the variance they explain: the first PC explains the highest variance, followed by the second PC and so on. So, we see that the first PC explains most the variance (72.3157%)  and the first two PCs explains almost all the variance (99.8466%) . However,  the third PC explains only 0.1486%. In such scenarios where we have many columns and the first few PCs account for most of the variability in the original data (such as 95% of the variance), we can use the first few PCs for data explorations and for machine learning.


> ![Figure.10](/image/pca_varianceexplain.PNG)  
> Figure 3.4.3.1
----

In data explorations, we can visualize the PCs and get a better understanding of processes using those PCs. From the figure below that using the first two PCs, it is unable to identify distinct clusters. Therefore it is not appropriate to use clustering analysis for this dataset.


> ![Figure.10](/image/pca_2pcs.PNG)  
> Figure 3.4.3.2
----
  
# **4.Conclusion** 
Nowadays, the climate change mainly caused by human activities is threatening seriously the sustainable development of human society. By conducting data analysis on this topic, we could clearly interpret the correlation between each climate factors, knowing the year-to-year trend of some climate index, finding the geographical pattern of weather data, and modeling for temperature prediction. 

According to the plots, there is an increasing trend of yearly average temperature since 1965, which is often called “global warming.” Besides, the average visibility distance shows a decreasing trend from 1950 to 1990. All these facts are sounding a cautionary note to people: you are expected to take actions immediately to protect the environment, such as footing instead of driving. 

Furthermore, there are some hidden facts between climate factors, which could never be known until analyzing the big dataset. The dew points, which means the temperature to which air must be cooled to become saturated with water vapor, are really important to the temperature. The temperature and elevation tend not to be correlated. The facts like those below could help climate scientists look for the new research ideas.

Simultaneously, we identify the factors which play a key role in temperature. Obviously, the predications results can be beneficial to the relative policy formulation to some extent.

Data science could be applied to the climate and weather field, and thus all these findings might be critical to environmental protection.
  
# **5.Future Work**



# References

[1] FEDERAL CLIMATE COMPLEX DATA DOCUMENTATION FOR INTEGRATED SURFACE DATA. https://www.ncei.noaa.gov/data/global-hourly/doc/isd-format-document.pdf.

[2] Smith, A., N. Lott, and R. Vose, 2011. The Integrated Surface Database: Recent Developments and Partnerships. Bulletin of the American Meteorological Society. https://journals.ametsoc.org/doi/abs/10.1175/2011BAMS3015.1

[3] Lott, J.N., R.S. Vose, S.A. Del Greco, T.R. Ross, S. Worley, and J.L. Comeaux, 2008. The integrated surface database: Partnerships and progress. 24th Conference on Interactive Information Processing Systems for Meteorology, Oceanography, and Hydrology (IIPS), New Orleans, LA, American Meteorological Society. https://ams.confex.com/ams/88Annual/webprogram/Paper131387.html

[4] Del Greco, S.A., J.N. Lott, R. Ray, D. Dellinger, F. Smith, and P. Jones, 2007. Surface data processing and integration at NOAA's National Climatic Data Center. 23rd Conference on Interactive Information Processing Systems for Meteorology, Oceanography, and Hydrology (IIPS), San Antonio, TX, American Meteorological Society. https://ams.confex.com/ams/87ANNUAL/webprogram/Paper116367.html





# Division of Labor

Yating Liang: Data Storage and Merging, Data Cleaning, EDA, Visualizations, and Writeup;

Xueyan Liu: Sampling subsets, Data Cleaning, EDA, Visualizations, and Writeup;

Jiahui Chen: Ridge regression, lasso regression, decision tree and gradient boosting tree model building, and Writeup;

Xinyao Mo: Generlized linear model and PCA model building, and Writeup.
