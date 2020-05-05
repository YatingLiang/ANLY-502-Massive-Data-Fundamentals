# **Project Information**

ANLY 502 Final Project

Course: ANLY 502 Massive Data Fundamentals Project 

Contributors: Jiahui Chen, Yating Liang, Xueyan Liu, Xinyao Mo


# **Dataset**

**NOAA Integrated Surface Database (ISD):** https://registry.opendata.aws/noaa-isd/ 

**Amazon Resource Name (ARN):** `arn:aws:s3:::noaa-global-hourly-pds` 

**Description:** The Integrated Surface Database (ISD) consists of global hourly and synoptic observations compiled from numerous sources into a gzipped fixed width format. The database includes over 35,000 stations worldwide from 1990 to 2019.



# **Executive Summary**



# **Introduction**

Our project is about weather data analysis. we used the NOAA Integrated Surface Database which contains Worldwide stations' hourly weather reports from 1900-2020. Instead of having an statistical academic goal. The main reason we choose this dataset is that it is good dataset that we have fun with data and we believe it is a good opportunity to explore the climatic and geographical knowledge by ourselves and to prove some hypothesis: such as whether the rain falls has changed over these years. 


......待补充

The first challenges we met is data cleaning since we have a very messy datasets. 
The second challenges is we need to process large-scale spatial data and convert to readable map.  
。。。。。。 待补充


# **Method**
* Data Cleaning  
Our dataset is extremely dirty. Our dataset is extremely dirty. Overall we  have 1TB total size and is consist of almost one million of small csv files in different folders and these files  has  multiple formats. So we split the data to multiple subsets.  
。。。。。。 待补充      
Another way we create a subset is simpling the station, and extract all available year’s report of this station. Even if from the same station, the reports’ formats are different. In order to obtain more complete data, we created a loop and merged these reports by columns. Thus by this new dataset, we are able to see the trends of various weather indicators. 
 
 
 
* Visualizations 
One of our project goals is discovering the geographical pattern of weather data in 2019. The best way to reach the goal is visualizing the data by a world map. However, it is not realistic to handle more than ten thousands of stations' data in the common data frame. Thus we introduced the Geospark.   Geospark is a cluster computing system for processing large-scale spatial data and is used by Mobike company, supporting their Real-time locating system. It contains several modules: core(RDD), SQL/Dataframe, Vis, and Zeppelin. (Python only contain RDD, and SQL for now) In python, GeoSpark is an extension of pyspark functions. Instead of common RDD, Geospark core provides 5 special SpatialRDD: PointRDD, PolygonRDD, LineStringRDD, CircleRDD, RectangleRDD. These RDD will automatically convert data to GeoData which have 2 attributes: geometry:  shapely geometry representation (consists of latitude and longitude ) as shapely objects; and userDate : string representation of other attributes.  For visualizing the data, we also implemented the libraries: Geopandas, Matplotlib and Seaborn. The Geopandas can handle the geospatial data, and it combines the capabilities of pandas and shapely. The Matplotlib and Seaborn allow us to create multiple types of visualizations from the data frame. 




# **Outcome** 
* Worldmap analysis  
The points on the world map show the locations of stations. (Figure 1: shows the elevation of each station.) Obviously, the distribution of weather stations is not uniform. In the United States and Europe, the density of stations is significantly higher than in other places. It looks like that if a country is richer, it may build more stations and conduct more academic research. We also noticed that the most stations are built at north temperate zone, the area has four distinct seasons. 
> ![Figure.1](/image/worldmap-EVELVATION.png)  
> Figure 1         
----
*Temperature*  
We observed the temperature from two aspects: Temperature variation and average temperature. (Figure 2: shows average temperature of each station in 2019, the blue points are the 50 coldest stations,  and the red points are the hottest stations. Figure 3: shows the temperature variation of each station in 2019.) 
> ![Figure.2](/image/worldmap-TEMP-WITH%20HIGH%20LOW%20station.png)  
> Figure 2 
> ![Figure.3](/image/worldmap-TEMP-VAR.png)  
> Figure 3 
----
According to the colour distribution, we can see the area with the higher average temperature tend to have less variation. The hottest stations are concentrated in low latitudes, the coldest stations are in high latitudes. Furthermore, we also found something surprised us. By common sense, we made a hypothesis that the annual average temperature of the station is lower while the elevation of the station is higher. However, from the graph, the distribution of elevation data and the distribution of average temperature is not similar. elevation and temperature don’t have a strong correlation.   

*Other weather indicators:*    
The following six figures respectively are: average precipitation，average visibility, average wind speed, average sea level pressure, average dew and average height of the lowest layer of clouds (ceiling height dimension) of each station in 2019. These plots offer us plenty of information:  Rainfall is greater in Asia than other continents, and inland seems to have more rainfall than coastal.Sea-level pressures vary widely in different regions of the world. The lower the longitude, the lower ceiling height dimension. Some climate indicators have similar color distributions, such as precipitation and sea level pressure, dew, visibility, ceiling height dimension, and temperature. These similarities represent the distinct climatic characteristics in different regions and the correlation between temperature and other weather indicators which help us select the variables for prediction.
> ![Figure.4](/image/worldmap-PRECIP.png)  
> Figure 4
> ![Figure.5](/image/worldmap-VIS.png)  
> Figure 5
> ![Figure.6](/image/worldmap-WIND.png)  
> Figure 6
> ![Figure.7](/image/worldmap-SLP.png)  
> Figure 7
> ![Figure.8](/image/worldmap-DEW.png)  
> Figure 8
> ![Figure.9](/image/worldmap-CIG.png)  
> Figure 9
----  

* Weather indicators trends   
待补充，。。。


* Variables selection   
For selecting variables for temperature prediction, we also introduced the heatmap of the correlation matrix (Figure 10). The heatmap illustrates the correlation between variables. Blue reflects the negative correlation while red reflects positive correlations. Darker colours indicate stronger correlations. According to the heatmap, we find the correlations between most the variables are weak.  Only a few of them holds a strong correlation which indicates that the low multicollinearity issue risk. Then we take look at the temperature’s correlation with other variables. By the sorted correlation coefficient, we selected wind speed, the height of the lowest layer of clouds, visibility, sea level pressure, precipitation and rainfall duration as our variables. (Figure 11 are the scatter plot of temperature with the other two variables. 
> ![Figure.10](/image/corr_full.png)  
> Figure 10   
> ![Figure.11](/image/SCA-TEMP-VS-DEW.png)  ![Figure.12](/image/SCA-TEMP-VS-ELE.png)
> Figure 11      
----
  
* Prediction     
待补充，。。。
  
# **Conclusion** 
  
  
