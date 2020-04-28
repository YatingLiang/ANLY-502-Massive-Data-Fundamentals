# **Project Information**

ANLY 502 Final Project

-（+*) Georgetown University Data Science and Analytics -（+*） Course: ANLY 502 Massive Data Fundamentals Project - （+*）Contributors: Jiahui Chen, Yating Liang, Xueyan Liu, Xinyao Mo

# **Dataset**

-（+*) **NOAA Integrated Surface Database (ISD):** https://registry.opendata.aws/noaa-isd/ 

-（+*) **· Amazon Resource Name (ARN):** `arn:aws:s3:::noaa-global-hourly-pds` 

-（+*) **Description:** The Integrated Surface Database (ISD) consists of global hourly and synoptic observations compiled from numerous sources into a gzipped fixed width format. The database includes over 35,000 stations worldwide from 1990 to 2019.

# **Introduction**


Data dictionary for reference:
https://docs.google.com/document/d/1GoEXECJQsrINVkIryOakTRnCPM-fipOaxkt0oQykNL0/edit?usp=sharing


Data indicator: 
  1. ELEVATION - 海拔
  2. WND 风的方向 + 风速
  3. VIS - 可见度
  4. TMP - 空气温度
  5. DEW - 露点温度
  6. SLP - 海平面气压
  7. AA1 - 降水measure的时常（第一位）   降水量 （第二位）
  8. AJ1 降雪
  9. KA1 极端温度时间
  10. IA2 地面最低温度
  11. MA1 大气压
  12. MD1 An indicator of the occurrence of the following items:  \\
        ATMOSPHERIC-PRESSURE-CHANGE tendency code\\
        ATMOSPHERIC-PRESSURE-CHANGE quality tendency code\\
        ATMOSPHERIC-PRESSURE-CHANGE three hour quantity\\
        ATMOSPHERIC-PRESSURE-CHANGE quality three hour code\\
        ATMOSPHERIC-PRESSURE-CHANGE twenty four hour quantity\\
        ATMOSPHERIC-PRESSURE-CHANGE quality twenty four hour code\\
        
  13. MW1 1，99不同的天气情况， categorical data 可选其中几个，作预测
  
  
  1. Challenge 1: collect data: 1TB dataset is consisted of around 1 million smalle csv, and storaged on the mutiples folders. These csv files have differnt format. solution: Sampleing the files, and merge to new data. Creating multiples subsets around 10GB by the question that we asked for projects
  
  2. Challenge 2. The most columns in dataset contains multiple infomation. Time consuming cleaning process.
  
  3. Challenge 3 visulization: HOW?
   
   
  
  
  
