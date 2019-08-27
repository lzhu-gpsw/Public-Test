# Databricks notebook source
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.figure_factory as ff
from plotly import tools as plotly_tools
from plotly.offline import plot


import pandas as pd
from IPython.display import display,HTML


# Set the maxium amount of data to print by default to 10 rows
pd.set_option('display.max_rows',10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### trying to create a branch

# COMMAND ----------

# update your username and API Key
userName = 'lzhu'
apiKey = 'CmNWVQEIRyYJMJNZSjnA'

def connect_plotly():
    plotly.tools.set_credentials_file(
    username=userName, api_key=apiKey)
    plotly.tools.set_config_file(plotly_domain='https://plotly.prod.dse.gopro-platform.com',
                             plotly_api_domain='https://plotly.prod.dse.gopro-platform.com',
                             plotly_streaming_domain='https://plotly.prod.dse.gopro-platform.com')

# COMMAND ----------

MAU_date = ['Oct 2018', 'Nov 2018', 'Dec 2018', 'Jan 2019','Feb 2019' ,'Mar 2019', 'Apr 2019','May 2019']

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *
# MAGIC From marketing.iso_country_lkup

# COMMAND ----------

df_CamModel = spark.sql('''
With cte As
(Select platform, 
        event_date,
        app_ver,
        model_number,
        custom_gopro_id,
        install_uuid,
        custom_mobile_device_id,
        custom_analytics_opt_in
From localytics.capture_events a
Join marketing.iso_country_lkup b
    On upper(a.country)=upper(b.code)
    and b.Standard = 'Y'
    and b.region = 'EMEA'
and a.event_date >= '2018-10-01' and a.event_date < '2019-06-01'
and a.event_name = "GoPro Device Connect") 

Select month
      ,Count(distinct case when model_number = 'HD7.01'  Then user_id Else Null End) As H7Black_Con
      ,Count(distinct case when model_number = 'H18.03' Then user_id Else Null End) As H7Silver_Con
      ,Count(distinct case when model_number = 'H18.02' Then user_id Else Null End) As H7White_Con
      ,Count(distinct case when model_number = 'H18.01' Then user_id Else Null End) As NewHERO_Con
      ,Count(distinct case when model_number Like 'FS%' Then user_id Else Null End) As FS_Con
      ,Count(distinct case when model_number = 'HD6.01' Then user_id Else Null End) As H6_Con     
      ,Count(distinct case when model_number = 'HD5.03' Then user_id Else Null End) As H5Session_Con
      ,Count(distinct case when model_number = 'HD5.02' Then user_id Else Null End) As H5Black_Con
      ,Count(distinct case when model_number in ('HX1.01','HD4.02','HD4.01') Then user_id Else Null End) As H4_Con
      ,Count(distinct case when model_number = 'HX1.01' Then user_id Else Null End) As H4Session_Con
      ,Count(distinct case when model_number = 'HD4.02' Then user_id Else Null End) As H4Black_Con
      ,Count(distinct case when model_number = 'HD4.01' Then user_id Else Null End) As H4Silver_Con
      ,Count(distinct case when model_number like 'HD3%' Then user_id Else Null End) As H3_Con
From
(
select distinct substr(event_date,1,7) as month
      ,'Android' as platform
      ,'>=4.2 and <4.5' as App_Version
      ,'OPT-In' As opt_in_status
      ,custom_gopro_id as user_id
      ,model_number 
      From  cte
      where platform = 'Android'
      and app_ver >= '4.2' and app_ver < '4.5'

Union

select distinct substr(event_date,1,7) as month
      ,'Android' as platform
       ,'Bug Version' as App_Version
       ,'OPT-Out' As opt_in_status
       ,install_uuid As user_id 
      ,model_number 
      from cte
      where platform = 'Android'
      and app_ver >=  '4.5' and app_ver < '4.5.2'
      and (custom_analytics_opt_in is null  or custom_analytics_opt_in = 'false')

Union

select distinct substr(event_date,1,7) as month
      ,'Android' as platform
       ,'Bug Version' as App_Version
       ,'OPT-In' As opt_in_status
       ,custom_mobile_device_id As user_id 
      ,model_number 
      from cte
      where platform = 'Android'
      and app_ver >=  '4.5' and app_ver < '4.5.2'
      and custom_analytics_opt_in = 'true' 

     
Union

select distinct substr(event_date,1,7) as month
      ,'Android' as platform
       ,'>= 4.5.2' as App_Version
       ,Case when custom_analytics_opt_in = 'true' Then 'OPT-In' When custom_analytics_opt_in = 'false' Then 'OPT-Out' Else Null End As opt_in_status
       ,custom_mobile_device_id As user_id
      ,model_number 
      from cte 
      where platform = 'Android'
      and app_ver >= '4.5.2'

Union

select distinct substr(event_date,1,7) as month
      ,'iOS' as platform
      ,'>= 4.3 and < 4.5' as App_Version
       ,Case when (custom_analytics_opt_in = 'TRUE' or custom_analytics_opt_in is Null) Then 'OPT-In' Else 'OPT-Out' End As opt_in_status
       ,custom_gopro_id As user_id
      ,model_number 
      from cte
      where platform = 'iOS'
      and app_ver >= '4.3' and app_ver < '4.5'

Union
  
 select distinct substr(event_date,1,7) as month
      ,'iOS' as platform
       ,'>= 4.5' as App_Version
       ,Case when custom_analytics_opt_in = 'TRUE' Then 'OPT-In' When custom_analytics_opt_in = 'FALSE' Then 'OPT-Out' Else Null End As opt_in_status
       ,custom_mobile_device_id as user_id 
      ,model_number 
      from cte
      where platform = 'iOS'
      and app_ver >= '4.5' 

) as sub
Group by 1
Order By 1
''')
df_CamModel = df_CamModel.toPandas()
print df_CamModel

# COMMAND ----------

H7Black = df_CamModel['H7Black_Con'].tolist()
H7Silver = df_CamModel['H7Silver_Con'].tolist()
H7White = df_CamModel['H7White_Con'].tolist()
NewHERO = df_CamModel['NewHERO_Con'].tolist()
FS = df_CamModel['FS_Con'].tolist()
H6 = df_CamModel['H6_Con'].tolist()
H5Black = df_CamModel['H5Black_Con'].tolist()
H5Session = df_CamModel['H5Session_Con'].tolist()
H4 = df_CamModel['H4_Con'].tolist()
H3 = df_CamModel['H3_Con'].tolist()

# COMMAND ----------

def num_to_k(df_num):
  text = []
  for num in df_num:
    text.append(str(int(round(num/1e3)))+'k')
  return text

# COMMAND ----------

connect_plotly()

trace_8 = go.Bar(x = MAU_date,
                 y = H7Black,
                 name = 'HERO 7 Black',
                 text = num_to_k(H7Black),
                 textposition = 'auto',
                 marker=dict(color='rgb(219,50,50)')
                )
trace_0 = go.Bar(x = MAU_date,
                 y = H7Silver,
                 name = 'HERO 7 Silver',
                 text = num_to_k(H7Silver),
                 textposition = 'auto',
                 marker=dict(color='rgba(219,50,50,0.6)')
                )
trace_1 = go.Bar(x = MAU_date,
                 y = H7White,
                 name = 'HERO 7 White',
                 text = num_to_k(H7White) ,
                 textposition = 'auto',
                 marker=dict(color='rgba(219,50,50,0.3)')
                )
trace_2 = go.Bar(x = MAU_date,
                 y = NewHERO,
                 name = 'New HERO',
                 text = num_to_k(NewHERO) ,
                 textposition = 'auto',
                 marker=dict(color='rgb(187,222,256)')
                )
trace_3 = go.Bar(x = MAU_date,
                 y = H6,
                 name = 'HERO 6',
                 text = num_to_k(H6) ,
                 textposition = 'auto',
                 marker=dict(color='rgba(50, 171, 96, 0.6)')                 
                )

trace_4 = go.Bar(x = MAU_date,
                 y = H5Black,
                 name = 'HERO 5 Black',
                 text = num_to_k(H5Black) ,
                 textposition = 'auto',
                 marker=dict(color='rgb(49,130,189)')
                )
trace_5 = go.Bar(x = MAU_date,
                 y = H5Session,
                 name = 'HERO 5 Session',
                 text = num_to_k(H5Session) ,
                 textposition = 'auto',
                 marker=dict(color='rgba(49,130,189,0.7)')
                )
trace_6 = go.Bar(x = MAU_date,
                 y = H4,
                 text = num_to_k(H4) ,
                 textposition = 'auto',
                 name = 'HERO 4 (All Models)',
                 marker=dict(color='rgba(249, 181, 34,0.6)')
                )

# trace_6 = go.Bar(x = MAU_date,
#                  y = Total_H4Session_Con,
#                  text = num_to_k(Total_H4Session_Con) ,
#                  textposition = 'auto',
#                  name = 'HERO 4 Session',
#                  marker=dict(color='rgb(249, 181, 34)')
#                 )

# trace_6_1 = go.Bar(x = MAU_date,
#                  y = Total_H4Black_Con,
#                  text = num_to_k(Total_H4Black_Con) ,
#                  textposition = 'auto',
#                  name = 'HERO 4 Black',
#                  #marker=dict(color='rgba(219, 99, 13,0.6)')
#                  marker=dict(color='rgb(219, 119, 13)')
#                 )
# trace_6_2 = go.Bar(x = MAU_date,
#                  y = Total_H4Silver_Con,
#                  text = num_to_k(Total_H4Silver_Con) ,
#                  textposition = 'auto',
#                  name = 'HERO 4 Silver',
#                  marker=dict(color='rgba(249, 181, 34,0.6)')
#                 )


trace_7 = go.Bar(x = MAU_date,
                 y = H3,
                 text = num_to_k(H3) ,
                 textposition = 'auto',
                 name = 'HERO 3 (All Models)',
                 marker=dict(color='rgb(176,180,197)')
                )

data = [trace_8,trace_0,trace_1,trace_2,trace_3,trace_4,trace_5,trace_6,trace_7][::-1]
#data = [trace_1,trace_2,trace_3,trace_4,trace_5,trace_6][::-1]


layout = go.Layout(
                title = 'Monthly Unique ConnectoRs to GP App by Camera Model<br>【EMEA】',
                yaxis = dict(title='ConnectoRs'),
                barmode = 'stack',
                autosize=False,
                width=1000,
                height=700
                )

fig = go.Figure(data=data,layout=layout)
py.iplot(fig,filename='Monthly_Report/Connectors_Model_EMEA',sharing='public')

p = plot({'data': data,'layout':layout}, output_type='div')
displayHTML(p)

# COMMAND ----------

H7 = [a+b+c for a,b,c in zip(H7Black,H7White,H7Silver)]
zipped_list = zip(H7,NewHERO,H6,H5Black,H5Session,H4,H3)
total = [sum(item) for item in zipped_list]
print total

# COMMAND ----------

def percent(cam_model,total):
  percent = []
  for i in range(len(total)):
    percent.append(round(cam_model[i]/float(total[i]),4))
  return percent

# COMMAND ----------

p_H7 = percent(H7,total)
p_H7Black =  percent(H7Black,total)
p_H7White =  percent(H7White,total)
p_H7Silver =  percent(H7Silver,total)
p_NewHERO = percent(NewHERO,total)
p_H6 = percent(H6,total)
p_H5Black = percent(H5Black,total)
p_H5Session = percent(H5Session,total)
p_H4 = percent(H4,total)
p_H3 = percent(H3,total)

# COMMAND ----------

def con_percent(per_list):
  text =[]
  for num in per_list:
    text.append("{0:.0f}%".format(num*100))
  return text

# COMMAND ----------

connect_plotly()


trace_8 = go.Bar(x = MAU_date,
                 y =  p_H7Black,
                 name = 'HERO 7 Black',
                 text =con_percent(p_H7Black) ,
                 textposition = 'auto',
                 marker=dict(color='rgb(219,50,50)')
                )
trace_0 = go.Bar(x = MAU_date,
                 y = p_H7White,
                 name = 'HERO 7 White',
                 text =con_percent(p_H7White) ,
                 textposition = 'auto',
                 marker=dict(color='rgba(219,50,50,0.6)')
                )
trace_1 = go.Bar(x = MAU_date,
                 y = p_H7Silver,
                 name = 'HERO 7 Silver',
                 text =con_percent(p_H7Silver) ,
                 textposition = 'auto',
                 marker=dict(color='rgba(219,50,50,0.3)')
                )
trace_2 = go.Bar(x = MAU_date,
                 y = p_NewHERO,
                 name = 'New HERO',
                 text =con_percent(p_NewHERO) ,
                 textposition = 'auto',
                 marker=dict(color='rgb(187,222,256)')
                )
trace_3 = go.Bar(x = MAU_date,
                 y = p_H6,
                 name = 'HERO 6',
                 text =con_percent(p_H6) ,
                 textposition = 'auto',
                 marker=dict(color='rgba(50, 171, 96, 0.6)')                 
                )

trace_4 = go.Bar(x = MAU_date,
                 y = p_H5Black,
                 name = 'HERO 5 Black',                 
                 text =con_percent(p_H5Black) ,
                 textposition = 'auto',
                 marker=dict(color='rgb(49,130,189)')
                # marker=dict(color='rgb(237,116,116)')
                )
trace_5 = go.Bar(x = MAU_date,
                 y = p_H5Session,
                 name = 'HERO 5 Session',
                 text =con_percent(p_H5Session) ,
                 textposition = 'auto',                 
                 marker=dict(color='rgba(49,130,189,0.7)')
                # marker=dict(color='rgb(219,50,50)')
                )
trace_6 = go.Bar(x = MAU_date,
                 y = p_H4,
                 name = 'HERO 4 (All Models)',
                 text =con_percent(p_H4) ,
                 textposition = 'auto',
                 marker=dict(color='rgb(239,209,91)')
                )
trace_7 = go.Bar(x = MAU_date,
                 y = p_H3,
                 text =con_percent(p_H3) ,
                 textposition = 'auto',
                 name = 'HERO 3 (All Models)',
                 marker=dict(color='rgb(176,180,197)')
                )

data = [trace_8,trace_0,trace_1,trace_2,trace_3,trace_4,trace_5,trace_6,trace_7][::-1]
#data = [trace_8,trace_2,trace_3,trace_4,trace_5,trace_6,trace_7][::-1]


layout = go.Layout(
                title = 'Camera Model Break-Down of Monthly Unique ConnectoRs to GP App <br> 【EMEA】',
                yaxis = dict(title='ConnectoRs Percentage'),
                barmode = 'stack',
                autosize=False,
                width=1000,
                height=700
                )

fig = go.Figure(data=data,layout=layout)
py.iplot(fig,filename='Monthly_Report/Connectors_Model_Percentage_EMEA',sharing='public')

p = plot({'data': data,'layout':layout}, output_type='div')
displayHTML(p)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ======================= Old Analysis ======================

# COMMAND ----------

MAU_date = ['Jul-17', 'Aug-17', 'Sep-17', 'Oct-17', 'Nov-17', 'Dec-17', 'Jan-18','Feb-18' ,'Mar-18', 'Apr-18','May-18','Jun-18','Jul-18']

# COMMAND ----------

# This function is to print number with commas as thousands separators
def chart_commas(num_list):
  text_list = []
  for i in range(len(num_list)):
    text_list.append("{:,}".format(num_list[i]))
  return text_list

# Convert datetime like '2017-10' to 'Oct, 2017'
def str_df(df):
  month = []
  for item in df:
    month.append(item.strftime("%b, %Y"))
  return month

# Convert number to percentage;
def num_per(num_list):
  per_list =[]
  for num in num_list:
    per_list.append("{0:.1f}%".format(num*100))
  return per_list

# COMMAND ----------

country_list = ('gr','ro','hu','bg','si','hr','rs','pt','es','it')

# COMMAND ----------

# MAGIC %sql
# MAGIC Refresh table cloud.account_state_with_email

# COMMAND ----------

# MAGIC %md
# MAGIC Monthly Active Camera ConnectoRs

# COMMAND ----------

df_CamCon = spark.sql('''
Select platform,
      month,
      Count(Distinct user_id) As CamCon,
      Count(Distinct Case when opt_in_status = 'OPT-In' Then user_id Else Null End) As Opt_In, 
      Count(Distinct Case when opt_in_status = 'OPT-Out' Then user_id Else Null End) As Opt_Out,
      Round(Count(Distinct Case when opt_in_status = 'OPT-In' Then user_id Else Null End)/Count(Distinct user_id),2) As Opt_In_Percent
From
(
select substr(event_date,1,7) as month
      ,'Android' as platform
      ,'Bug Free Version >= 4.2' as `App_Version`
      ,'OPT-In' As opt_in_status
      ,custom_gopro_id as `user_id` 
      from cloud.account_state_with_email c
      Inner Join localytics.capture_events a
      On a.custom_gopro_id = c.record_gopro_user_id and (c.record_email is not null and c.record_email <> '')
      where platform = 'Android'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver >= '4.2'
      and event_name = "GoPro Device Connect"
      and country = 'rs'


Union

select substr(event_date,1,7) as month
      ,'Android' as platform
       ,'Bug Version >= 4.2' as `App_Version`
       ,'OPT-Out' As opt_in_status
       ,install_uuid As `user_id` 
      from localytics.capture_events 
      where platform = 'Android'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver in  ('4.5','4.5.0.1','4.5.0.2','4.5.1')
      and event_name = "GoPro Device Connect"
      and (custom_analytics_opt_in is null or lower(custom_analytics_opt_in) <> 'true') 
      and country = 'rs'
     
Union

select substr(event_date,1,7) as month
      ,'Android' as platform
       ,'Bug Free Version >= 4.2' as `App_Version`
       ,'OPT-Out' As opt_in_status
       ,custom_mobile_device_id As `user_id` 
      from localytics.capture_events 
      where platform = 'Android'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver >= '4.5.2'
      and event_name = "GoPro Device Connect"
      and (custom_analytics_opt_in is null or lower(custom_analytics_opt_in) <> 'true')
      and country = 'rs' 

Union


 select substr(event_date,1,7) as month
      ,'iOS' as platform
      ,'App Ver >= 4.3 OPT-In' as `App_Version`
       ,'OPT-In' As opt_in_status
       ,custom_gopro_id As `user_id` 
      from cloud.account_state_with_email c
      Inner Join localytics.capture_events a
      On a.custom_gopro_id = c.record_gopro_user_id and (c.record_email is not null and c.record_email <> '')
      where platform = 'iOS'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver >= '4.3' 
      and event_name = "GoPro Device Connect"
      and country = 'rs'


Union
  
 select substr(event_date,1,7) as month
      ,'iOS' as platform
       ,'App Ver >= 4.3 OPT-Out' as `App_Version`
       ,'OPT-Out' As opt_in_status
       ,custom_mobile_device_id as `user_id` 
      from localytics.capture_events 
      where platform = 'iOS'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver >= '4.5' 
      and event_name = "GoPro Device Connect"
      and (custom_analytics_opt_in is null or lower(custom_analytics_opt_in) <> 'true')
      and country = 'rs'

) as sub
Group by 1,2
Order By 1,2
''')
df_CamCon = df_CamCon.toPandas()

# COMMAND ----------

df_Android_CamCon = df_CamCon[df_CamCon['platform'] == 'Android']
df_iOS_CamCon = df_CamCon[df_CamCon['platform'] == 'iOS']

Android_CamCon = df_Android_CamCon['CamCon'].tolist()
iOS_CamCon = df_iOS_CamCon['CamCon'].tolist()
Total_CamCon = [a+b for a,b in zip(Android_CamCon,iOS_CamCon)]

# COMMAND ----------

trace_1 = go.Scatter(x = MAU_date[10:],
                     y = Total_CamCon,
                     mode = 'lines+markers+text',
                     line = dict(color = 'rgb(244,90,100)',width = 2),
                     marker = dict(symbol ='circle',size=8),
                     text = chart_commas(Total_CamCon),
                     textposition='top',
                     textfont=dict(family='sans serif',size=10),
                     name = 'Total ConnectoRs'
                    )

trace_2 = go.Bar(x = MAU_date[10:],
                 y = Android_CamCon,
                 name = 'Android ConnectoRs',
                 text = chart_commas(Android_CamCon),
                 marker=dict(color='rgb(49,130,189)')
                )
trace_3 = go.Bar(x = MAU_date[10:],
                 y = iOS_CamCon,
                 name = 'iOS ConnectoRs',
                 text = chart_commas(iOS_CamCon),
                 marker=dict(color='rgb(204,204,204)')
                )
layout = go.Layout(
                title = '【Serbia】Monthly Camera ConnectoRs - GoPro App',
                #xaxis = dict(tickangle = -45),
                yaxis = dict(title='ConnectoRs'),
                barmode = 'group',
                autosize=False,
                width=600,
                height=400
                )

data = [trace_1,trace_2,trace_3]

fig = go.Figure(data=data,layout=layout)
py.iplot(fig,filename='EMEA_Tracking/Serbia_CamCon',sharing='public')

p = plot({'data': data,'layout':layout}, output_type='div')
displayHTML(p)

# COMMAND ----------

# MAGIC %md
# MAGIC Model Break-down

# COMMAND ----------

df_Model = spark.sql('''
Select month
      ,count(distinct user_id) As Total_Con
      ,Round(Count(distinct case when model_number = 'H18.01' Then user_id Else Null End)/count(distinct user_id),3) As NewHERO_Per
      ,Round(Count(distinct case when model_number Like 'FS%' Then user_id Else Null End)/count(distinct user_id),3) As FS_Per
      ,Round(Count(distinct case when model_number = 'HD6.01' Then user_id Else Null End)/count(distinct user_id),3) As H6_Per     
      ,Round(Count(distinct case when model_number like 'HD5%' Then user_id Else Null End)/count(distinct user_id),3) As H5_Per
      ,Round(Count(distinct case when model_number like 'HD4%' or model_number = 'HX1.01' Then user_id Else Null End)/count(distinct user_id),3) As H4_Per
      ,Round(Count(distinct case when model_number like 'HD3%' Then user_id Else Null End)/count(distinct user_id),3) As H3_Per
From
(
select substr(event_date,1,7) as month
      ,'Android' as platform
      ,'Bug Free Version >= 4.2' as `App_Version`
      , model_number
      ,'OPT-In' As opt_in_status
      ,custom_gopro_id as `user_id` 
      from cloud.account_state_with_email c
      Inner Join localytics.capture_events a
      On a.custom_gopro_id = c.record_gopro_user_id and (c.record_email is not null and c.record_email <> '')
      where platform = 'Android'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver >= '4.2'
      and event_name = "GoPro Device Connect"
and country = 'it'

Union

select substr(event_date,1,7) as month
      ,'Android' as platform
       ,'Bug Version >= 4.2' as `App_Version`
      , model_number
       ,'OPT-Out' As opt_in_status
       ,install_uuid As `user_id` 
      from localytics.capture_events 
      where platform = 'Android'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver in  ('4.5','4.5.0.1','4.5.0.2','4.5.1')
      and event_name = "GoPro Device Connect"
      and (custom_analytics_opt_in is null or lower(custom_analytics_opt_in) <> 'true') 
and country = 'it'
     
Union

select substr(event_date,1,7) as month
      ,'Android' as platform
       ,'Bug Free Version >= 4.2' as `App_Version`
       , model_number
      ,'OPT-Out' As opt_in_status
       ,custom_mobile_device_id As `user_id` 
      from localytics.capture_events 
      where platform = 'Android'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver >= '4.5.2'
      and event_name = "GoPro Device Connect"
      and (custom_analytics_opt_in is null or lower(custom_analytics_opt_in) <> 'true') 
and country = 'it'

Union


 select substr(event_date,1,7) as month
      ,'iOS' as platform
      ,'App Ver >= 4.3 OPT-In' as `App_Version`
      , model_number
       ,'OPT-In' As opt_in_status
       ,custom_gopro_id As `user_id` 
      from cloud.account_state_with_email c
      Inner Join localytics.capture_events a
      On a.custom_gopro_id = c.record_gopro_user_id and (c.record_email is not null and c.record_email <> '')
      where platform = 'iOS'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver >= '4.3' 
      and event_name = "GoPro Device Connect"
and country = 'it'


Union
  
 select substr(event_date,1,7) as month
      ,'iOS' as platform
      ,'App Ver >= 4.3 OPT-Out' as `App_Version`
       , model_number
       ,'OPT-Out' As opt_in_status
       ,custom_mobile_device_id as `user_id` 
      from localytics.capture_events 
      where platform = 'iOS'
      and event_date >= '2018-05-01' and event_date < '2018-08-01'
      and app_ver >= '4.5' 
      and event_name = "GoPro Device Connect"
      and (custom_analytics_opt_in is null or lower(custom_analytics_opt_in) <> 'true')
and country = 'it'

) as sub
Group by 1
Order By 1
''')
df_Model = df_Model.toPandas()
print df_Model

# COMMAND ----------

NewHERO_Per = df_Model['NewHERO_Per'].tolist()
FS_Per = df_Model['FS_Per'].tolist()
H6_Per = df_Model['H6_Per'].tolist()
H5_Per = df_Model['H5_Per'].tolist()
H4_Per = df_Model['H4_Per'].tolist()
H3_Per = df_Model['H3_Per'].tolist()

# COMMAND ----------

Con_Date = MAU_date[10:]

trace_0 = go.Bar(x = Con_Date,
                 y = NewHERO_Per,
                 name = 'New HERO',
                 marker=dict(color='rgba(50, 171, 96)'),
                 text = num_per(NewHERO_Per)
                )
trace_1 = go.Bar(x = Con_Date,
                 y = H6_Per,
                 name = 'HERO 6',
                 marker=dict(color='rgba(50, 171, 96, 0.7)'),
                 text = num_per(H6_Per)
                )
trace_2 = go.Bar(x = Con_Date,
                 y = H5_Per,
                 name = 'HERO 5',
                 marker=dict(color='rgba(100,181,246,0.8)'),
                 text = num_per(H5_Per)
                )

trace_3 = go.Bar(x = Con_Date,
                 y = H4_Per,
                 name = 'HERO 4',
                 marker=dict(color='rgb(237,116,116)'),
                 text = num_per(H4_Per)                
                )
trace_4 = go.Bar(x = Con_Date,
                 y = H3_Per,
                 name = 'HERO 3',
                 marker=dict(color='rgba(239,209,91,0.8)'),
                 text = num_per(H3_Per)
                )
trace_5 = go.Bar(x = Con_Date,
                 y = FS_Per,
                 name = 'Fusion',
                 marker=dict(color='rgb(176,180,197)'),
                 text = num_per(FS_Per)
                )

data = [trace_0,trace_1,trace_2,trace_3,trace_4,trace_5][::-1]
#data = [trace_1,trace_2,trace_3,trace_4,trace_5,trace_6][::-1]


layout = go.Layout(
                title = '【Italy】Unique ConnectoRs Percentage by Camera Model<br> 【iOS and Android Aggregated】',
                yaxis = dict(title='Camera Model Percentage'),
                barmode = 'stack',
                autosize=False,
                width=600,
                height=400
                )

fig = go.Figure(data=data,layout=layout)
py.iplot(fig,filename='EMEA_Tracking/Italy_Model_Percent',sharing='public')

p = plot({'data': data,'layout':layout}, output_type='div')
displayHTML(p)

# COMMAND ----------

