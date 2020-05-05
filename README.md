# **Project Information**

ANLY 502 Final Project

Course: ANLY 502 Massive Data Fundamentals Project 

Contributors: Jiahui Chen, Yating Liang, Xueyan Liu, Xinyao Mo

Code files：待补充


# **Dataset**

**NOAA Integrated Surface Database (ISD):** https://registry.opendata.aws/noaa-isd/ 

**Amazon Resource Name (ARN):** `arn:aws:s3:::noaa-global-hourly-pds` 

**Description:** The Integrated Surface Database (ISD) consists of global hourly and synoptic observations compiled from numerous sources into a gzipped fixed width format. The database includes over 35,000 stations worldwide from 1990 to 2019.



# **Executive Summary**

待补充

# **1.Introduction**

Our project is about weather data analysis. we used the NOAA Integrated Surface Database which contains Worldwide stations' hourly weather reports from 1900-2020. Instead of having an statistical academic goal. The main reason we choose this dataset is that it is good dataset that we have fun with data and we believe it is a good opportunity to explore the climatic and geographical knowledge by ourselves and to prove some hypothesis: such as whether the rain falls has changed over these years. 


......待补充

The first challenges we met is data cleaning since we have a very messy datasets. 
The second challenges is we need to process large-scale spatial data and convert to readable map.  
。。。。。。 待补充


# **2.Method** 

## 2.1 Data Cleaning  
Our dataset is extremely dirty. Our dataset is extremely dirty. Overall we  have 1TB total size and is consist of almost one million of small csv files in different folders and these files  has  multiple formats. So we split the data to multiple subsets.  
。。。。。。 待补充      
Another way we create a subset is simpling the station, and extract all available year’s report of this station. Even if from the same station, the reports’ formats are different. In order to obtain more complete data, we created a loop and merged these reports by columns. Thus by this new dataset, we are able to see the trends of various weather indicators. 
 
 
 
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
The points on the world map show the locations of stations. (Figure 1: shows the elevation of each station.) Obviously, the distribution of weather stations is not uniform. In the United States and Europe, the density of stations is significantly higher than in other places. It looks like that if a country is richer, it may build more stations and conduct more academic research. We also noticed that the most stations are built at north temperate zone, the area has four distinct seasons. 
> ![Figure.1](/image/worldmap-EVELVATION.png)  
> Figure 3.1.1         
----
*Temperature*  
We observed the temperature from two aspects: Temperature variation and average temperature. (Figure 2: shows average temperature of each station in 2019, the blue points are the 50 coldest stations,  and the red points are the hottest stations. Figure 3: shows the temperature variation of each station in 2019.) 
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
待补充，。。。


## 3.3 Variables selection   
For selecting variables for temperature prediction, we also introduced the heatmap of the correlation matrix (Figure 10). The heatmap illustrates the correlation between variables. Blue reflects the negative correlation while red reflects positive correlations. Darker colours indicate stronger correlations. According to the heatmap, we find the correlations between most the variables are weak.  Only a few of them holds a strong correlation which indicates that the low multicollinearity issue risk. Then we take look at the temperature’s correlation with other variables. By the sorted correlation coefficient, we selected wind speed, the height of the lowest layer of clouds, visibility, sea level pressure, precipitation and rainfall duration as our variables. (Figure 11 are the scatter plot of temperature with the other two variables.)
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


<img width="300" height="150" src="/image/p&a_ridge.PNG"/> <img width="300" height="150" src="/image/res_ridge.PNG" alt="图片描述文字"/>
> Figure 3.4.1


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


  
# **4.Conclusion** 
  
  
