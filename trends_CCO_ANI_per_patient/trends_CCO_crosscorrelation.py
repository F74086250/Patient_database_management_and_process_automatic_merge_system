import os
import pandas as pd
import openpyxl
import math
import re
from datetime import datetime
from datetime import timedelta
import csv
import time
import warnings
import numpy as np
warnings.filterwarnings("ignore")


def read_time_gap_record_data(fi):
  df=pd.read_excel(fi)
  SerialNumber=df['SerialNumber'].tolist()
  for i in range(len(SerialNumber)):
    SerialNumber[i]="0"+str(SerialNumber[i])
  df['SerialNumber']=SerialNumber
  filt=(df['is_the_data_complete?']=='y')
  df_filt=(df.loc[filt])


  SerialNum_003Num=[]
  CCO_time_gap=[]
  ANI_time_gap=[]
  patient_name=[]
  for i in range(len(df_filt)):
    tmp=(df_filt.iloc[i]['ResearchSerialNumber'].split("-"))[1]
    SerialNum_003Num.append(df_filt.iloc[i]['SerialNumber']+"_"+tmp)
    CCO_time_gap.append(df_filt.iloc[i]['CCO_time_gap'])
    ANI_time_gap.append(df_filt.iloc[i]['ANI_time_gap'])
    patient_name.append(df_filt.iloc[i]['Name'])


  Trends_filename=[]
  CCO_filename=[]
  ANI_filename=[]
  for i in range(len(SerialNum_003Num)):
    Trends_filename_tmp=[]
    CCO_filename_tmp=[]
    ANI_filename_tmp=[]
    folder_name="./"+SerialNum_003Num[i]
    all_file=os.listdir(folder_name)
    for j in range(len(all_file)):
      if "trends" in all_file[j]:
        Trends_filename_tmp.append(folder_name+"/"+all_file[j])
      elif "CCO" in all_file[j]:
       CCO_filename_tmp.append(folder_name+"/"+all_file[j])
      elif "ANI" in all_file[j]:
        ANI_filename_tmp.append(folder_name+"/"+all_file[j])
    Trends_filename.append(Trends_filename_tmp)
    CCO_filename.append(CCO_filename_tmp)
    ANI_filename.append(ANI_filename_tmp)
  check_if_all_files_exist(Trends_filename,CCO_filename,ANI_filename)
  return Trends_filename,CCO_filename,ANI_filename,CCO_time_gap,ANI_time_gap,SerialNum_003Num,patient_name




def check_if_all_files_exist(Trends_filename,CCO_filename,ANI_filename):
  not_exist=0
  not_exist_filename=[]
  for i in range(len(Trends_filename)):
    for j in range(len(Trends_filename[i])):
      if os.path.isfile(Trends_filename[i][j]):
        print(f"File exists,file name is {Trends_filename[i][j]}")
      else:
        not_exist=1
        not_exist_filename.append(Trends_filename[i][j])
        print("File does not exist!")
  for i in range(len(CCO_filename)):
    for j in range(len(CCO_filename[i])):
      if os.path.isfile(CCO_filename[i][j]):
        print(f"File exists,file name is {CCO_filename[i][j]}")
      else:
        not_exist=1
        not_exist_filename.append(CCO_filename[i][j])
        print("File does not exist!")
  for i in range(len(ANI_filename)):
    for j in range(len(ANI_filename[i])):
      if os.path.isfile(ANI_filename[i][j]):
        print(f"File exists,file name is {ANI_filename[i][j]}")
      else:
        not_exist=1
        not_exist_filename.append(ANI_filename[i][j])
        print("File does not exist!")
  if not_exist==0:
    print("\nSuccessful! All files exist and can be merged!\n")
  else:
    print("\nSome files are missing, please check!")
    print("The missing file name is:")
    for i in range(len(not_exist_filename)):
      print(not_exist_filename[i])


def read_trends_and_CCO_without_time_gap_shift(SerialNumber_003):
  trends_df_list=[]
  CCO_without_time_gap_shift_df_list=[]
  for i in range(len(SerialNumber_003)):
    trends_file_name="./Trends_csv/"+SerialNumber_003[i]+"_trends.csv"
    CCO_without_time_gap_shift_csv_file_name="./CCO_without_time_gap_shift_csv/"+SerialNumber_003[i]+"_CCO_without_time_gap_shift.csv"

    trends_data=[]
    with open(trends_file_name,newline='',encoding="utf-8") as csvfile:
      rows=csv.reader(csvfile)
      for row in rows:
        trends_data.append(row)
    trends_column_name=trends_data[0]
    del trends_data[0]
    trends_df_list.append(pd.DataFrame(trends_data,columns=trends_column_name))
    

    CCO_without_time_gap_shift_data=[]
    with open(CCO_without_time_gap_shift_csv_file_name, newline='',encoding="utf-8") as csvfile:
      rows = csv.reader(csvfile)
      for row in rows:
        CCO_without_time_gap_shift_data.append(row)
    CCO_without_time_gap_shift_column_name=CCO_without_time_gap_shift_data[0]
    del CCO_without_time_gap_shift_data[0]
    CCO_without_time_gap_shift_df_list.append(pd.DataFrame(CCO_without_time_gap_shift_data,columns=CCO_without_time_gap_shift_column_name))
  # print(trends_df_list)
  # print(CCO_without_time_gap_shift_df_list)
  return trends_df_list,CCO_without_time_gap_shift_df_list

def fetch_HR_and_BP(trends_df_list,CCO_without_time_gap_shift_df_list,CCO_time_gap):
  trends_df_only_time_HR_BP_list=[]
  CCO_df_only_time_HR_BP_list=[]
  for i in range(len(trends_df_list)):
    trends_df_list[i]=trends_df_list[i].rename(columns={'﻿Time':'Time'})
    trends_df_only_time_HR_BP=trends_df_list[i][['Time','HR','P1mean']]
    trends_df_only_time_HR_BP_list.append(trends_df_only_time_HR_BP)
    CCO_without_time_gap_shift_df_list[i]=CCO_without_time_gap_shift_df_list[i].rename(columns={'﻿Time':'Time'})
    CCO_df_only_time_HR_BP=CCO_without_time_gap_shift_df_list[i][['Time','平均脉搏速率(次/分)','平均血压(mmHg)']]
    CCO_df_only_time_HR_BP_list.append(CCO_df_only_time_HR_BP)
    trends_df_only_time_HR_BP_list[i].replace("None",0.0,inplace=True)
    CCO_df_only_time_HR_BP_list[i].replace("None",0.0,inplace=True)

  print("Using cross-correlation to predict real CCO start time by HR:\n")
  for i in range(len(trends_df_only_time_HR_BP_list)):
    trends_HR=trends_df_only_time_HR_BP_list[i]['HR'].tolist()
    CCO_HR=CCO_df_only_time_HR_BP_list[i]['平均脉搏速率(次/分)'].tolist()
    
    float_trends_HR = [float(j) for j in trends_HR]
    float_CCO_HR=[float(j) for j in CCO_HR]

    x = np.array(float_trends_HR)
    h = np.array(float_CCO_HR)
    y=np.correlate(x,h,'full')
    y=y.tolist()
    
    # print("Trends start time: "+trends_df_only_time_HR_BP_list[i]['Time'][0])
    print("Real CCO start time: "+CCO_df_only_time_HR_BP_list[i]['Time'][0]+":00 "+CCO_time_gap[i])
    # print(y.index(max(y))-len(h))
    # print(CCO_time_gap[i])
    CCO_start_time_in_trends=trends_df_only_time_HR_BP_list[i]['Time'][0]
    CCO_start_time_in_trends=pd.Timestamp(CCO_start_time_in_trends)
    CCO_start_time_in_trends=(CCO_start_time_in_trends+timedelta(minutes=y.index(max(y))-len(h)))
    print("Predict CCO start time:",end='')
    print(CCO_start_time_in_trends.strftime("%H:%M:%S"))
    print("\n\n")

  print("==========================================\n")
  print("Using cross-correlation to predict real CCO start time by BP:\n")
  for i in range(len(trends_df_only_time_HR_BP_list)):
    trends_BP=trends_df_only_time_HR_BP_list[i]['P1mean'].tolist()
    CCO_BP=CCO_df_only_time_HR_BP_list[i]['平均血压(mmHg)'].tolist()
    
    float_trends_BP = [float(j) for j in trends_BP]
    float_CCO_BP=[float(j) for j in CCO_BP]

    x = np.array(float_trends_BP)
    h = np.array(float_CCO_BP)
    y=np.correlate(x,h,'full')
    y=y.tolist()
    # print("Trends start time: "+trends_df_only_time_HR_BP_list[i]['Time'][0])
    print("Real CCO start time: "+CCO_df_only_time_HR_BP_list[i]['Time'][0]+":00 "+CCO_time_gap[i])
    # print(y.index(max(y))-len(h))
    # print(CCO_time_gap[i])
    CCO_start_time_in_trends=trends_df_only_time_HR_BP_list[i]['Time'][0]
    CCO_start_time_in_trends=pd.Timestamp(CCO_start_time_in_trends)
    CCO_start_time_in_trends=(CCO_start_time_in_trends+timedelta(minutes=y.index(max(y))-len(h)))
    print("Predict CCO start time:",end='')
    print(CCO_start_time_in_trends.strftime("%H:%M:%S"))
    print("\n\n")

  return
    


Trends_filename,CCO_filename,ANI_filename,CCO_time_gap,ANI_time_gap,SerialNumber_003,patient_name=read_time_gap_record_data("time_gap_record.xlsx")
trends_df_list,CCO_without_time_gap_shift_df_list=read_trends_and_CCO_without_time_gap_shift(SerialNumber_003)
fetch_HR_and_BP(trends_df_list,CCO_without_time_gap_shift_df_list,CCO_time_gap)



