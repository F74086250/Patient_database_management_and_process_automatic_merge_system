import pandas as pd
import openpyxl
import os
import math
import re
from datetime import datetime
from datetime import timedelta
import csv
import time
import warnings
warnings.filterwarnings("ignore")
##################################################################################################################################################################################################################################################################################################################################################
###################################################################________All_data_read________##########################################################################################################################################################################################################################################################################################################################################################################################################################
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



##################################################################################################################################################################################################################################################################################################################################################
###################################################################________Trends_processing________#######################################################################################################################################################################################################################################################################################################################################################################################################################

def Trends_processing(Trends_filename,row_num):
  Trends_df_list=[]
  for i in range(len(Trends_filename)):
    f=open(Trends_filename[i],'r')
    data=[]
    for i in f:
      data.append(i.rstrip())

    # print(len(data))
    data_arr=[]
    for i in data:
      i=i.split("\t")
      data_arr.append(i)
    del data_arr[0]
    column_name=data_arr[0]
    del data_arr[0]
    df=pd.DataFrame(data_arr,columns=column_name)
    f.close()
    
    df=trends_remove_outliers(df)
    df=trends_avg(df,row_num)
    Trends_df_list.append(df)
  if len(Trends_df_list)==1:
    return Trends_df_list[0]
  else:
    df_vertical_merge=Trends_df_vertical_merge(Trends_df_list)
    return df_vertical_merge


def trends_remove_outliers(df):
  range_dic={}
  range_dic['HR']=[40.0,100.0]	
  range_dic['ST1']=[0.1,1.0]	
  range_dic['ST2']=[0.1,1.0]
  range_dic['ST3']=[-0.5,0.0]
  range_dic['Imped.']=[0.0,0.0]
  range_dic['P1sys']=[80.0,170.0]	
  range_dic['P1dia']=[40.0,90.0]
  range_dic['P1mean']=[70.0,120.0]
  range_dic['PR(P1)']=[50.0,80.0]
  range_dic['P2sys']=[130.0,140.0]
  range_dic['P2dia']=[130.0,140.0]
  range_dic['P2mean']=[130.0,140.0]
  range_dic['PR(P2)']=[0.0,0.0]
  range_dic['P3sys']=[0.0,0.0]
  range_dic['P3dia']=[0.0,0.0]
  range_dic['P3mean']=[0.0,0.0]
  range_dic['PR(P3)']=[0.0,0.0]
  range_dic['P4sys']=[0.0,0.0]
  range_dic['P4dia']=[0.0,0.0]
  range_dic['P4mean']=[0.0,0.0]
  range_dic['PR(P4)']=[0.0,0.0]
  range_dic['NIBPsys']=[80.0,170.0]
  range_dic['NIBPdia']=[50.0,100.0]
  range_dic['NIBPmean']=[70.0,120.0]
  range_dic['PR(NIBP)']=[0.0,0.0]
  range_dic['T1']=[30.0,40.0]
  range_dic['T2']=[30.0,40.0]
  range_dic['SpO2']=[90.0,100.0]
  range_dic['PR(SpO2)']=[1.0,100.0]	
  range_dic['SpO2_ir']=[0.0,8.0]
  range_dic['EtCO2']=[30.0,50.0]
  range_dic['FiCO2']=[0.0,2.0]
  range_dic['RR(CO2)']=[1.0,20.0]
  range_dic['Pamb']=[750.0,760.0]
  range_dic['FeO2']=[25.0,40.0]
  range_dic['FiO2']=[30.0,40.0]
  range_dic['FeN2O']=[0.01,0.1]
  range_dic['FiN2O']=[0.0,0.05]
  range_dic['FeAA']=[0.01,2.0]
  range_dic['FiAA']=[1.0,2.0]
  range_dic['MAC']=[0.5,0.9]
  range_dic['RR(Spiro)']=[0.0,0.0]	
  range_dic['Ppeak']=[19.0,30.0]
  range_dic['PEEP']=[5.0,7.0]
  range_dic['Pplat']=[0.0,0.0]
  range_dic['TVinsp']=[300.0,500.0]
  range_dic['TVexp']=[300.0,500.0]
  range_dic['Compl']=[0.0,0.0]
  range_dic['MVex']=[0.1,7.0]
  range_dic['T1%']=[0.0,3.0]
  range_dic['TOF%']=[80.0,110.0]
  range_dic['PTC']=[20000.0,27000.0]
  range_dic['HR(ECG)']=[50.0,140.0]
  range_dic['HRmax']=[50.0,140.0]
  range_dic['HRmin']=[50.0,140.0]
  range_dic['P5sys']=[0.0,0.0]
  range_dic['P5dia']=[0.0,0.0]
  range_dic['P5mean']=[0.0,0.0]
  range_dic['PR(P5)']=[0.0,0.0]
  range_dic['P6sys']=[0.0,0.0]
  range_dic['P6dia']=[0.0,0.0]
  range_dic['P6mean']=[0.0,0.0]
  range_dic['PR(P6)']=[0.0,0.0]
  range_dic['Marker']=[0.0,0.0]
  range_dic['HR(aECG)']=[50.0,130.0]
  range_dic['RRt(aECG)']=[0.0,0.0]
  range_dic['PVC']=[0.0,0.0]
  range_dic['aStatus']=[0.0,0.0]
  range_dic['ST(I)']=[0.0,0.0]
  range_dic['ST(II)']=[40.0,50.0]
  range_dic['ST(III)']=[0.0,0.5]
  range_dic['ST(AVL)']=[0.0,2.0]
  range_dic['NMT(Count)']=[0.0,10.0]	
  range_dic['NMT(R1)']=[0.0,130.0]
  range_dic['NMT(R2)']=[0.0,30.0]
  range_dic['NMT(R3)']=[0.0,120.0]
  range_dic['NMT(R4)']=[0.0,0.0]
  range_dic['FEMG']=[0.0,0.0]
  range_dic['BIS']=[0.0,0.0]
  range_dic['BisSQI']=[0.0,0.0]
  range_dic['BISEMG']=[0.0,0.0]
  range_dic['BISSR']=[0.0,0.0]
  range_dic['SE']=[1.0,100.0]
  range_dic['RE']=[1.0,100.0]
  range_dic['BSR']=[0.0,0.0]
  range_dic['VO2']=[0.0,0.0]
  range_dic['VCO2']=[0.0,0.0]
  range_dic['PEEPi']=[0.0,0.0]	
  range_dic['Pmean']=[8.0,13.0]
  range_dic['Raw']=[0.0,0.0]
  range_dic['MVinsp']=[0.0,0.0]
  range_dic['PEEPep']=[0.0,0.0]
  range_dic['MVsp']=[0.1,7.0]
  range_dic['I:E']=[0.0,0.0]
  range_dic['Tinsp']=[0.0,0.0]
  range_dic['Texp']=[0.0,0.0]
  range_dic['StCompl']=[0.0,0.0]	
  range_dic['StPplat']=[0.0,0.0]
  range_dic['StPEEPe']=[0.0,0.0]
  range_dic['StPEEPi']=[0.0,0.0]
  range_dic['FeBal']=[58.0,70.0]
  range_dic['FiBal']=[0.0,0.0]
  range_dic['SPV']=[2.0,8.0]
  range_dic['PPV']=[1.0,13.0]
  range_dic['SPI']=[1.0,100.0]
  range_dic['SpO2(2)']=[0.0,0.0]	
  range_dic['P8mean']=[0.0,0.0]
  range_dic['MACage']=[0.6,0.9]
  # cnt=0
  for i in df:
    if i=="Time":
      continue
    data_list=df[i].tolist()
    for j in range(len(data_list)):
      data_list[j]=float(data_list[j])
      if data_list[j]>range_dic[i][1] or data_list[j]<range_dic[i][0]:
        data_list[j]="No Data"
        # cnt+=1
    df[i]=data_list
  return df
def is_number(num):
  pattern=re.compile(r'(.*)\.(.*)\.(.*)')
  if pattern.match(num):
    return False
  return num.replace(".","").isdigit()
def mean(lst):
  sum_num=0.0
  int_num=0
  for i in lst:
    if is_number(str(i)):
      sum_num+=i
      int_num+=1
  if int_num==0:
    return "None"
  return round(sum_num/int_num,2)
def trends_df_avg(df):
  one_row_list=[]
  for i in df:
    if i=="Time":
      now_time_list=(df[i].tolist())[0].split(":")
      now_time=now_time_list[0]+":"+now_time_list[1]
      one_row_list.append(now_time)
      continue
    temp_list=df[i].tolist()
    one_row_list.append(mean(temp_list))

  return one_row_list

def trends_avg(df,row_num):
  start1=0
  end1=0
  cnt=1
  for i in range(len(df['Time'])):
    # print(df['Time'][i][0:5])
    
    if df['Time'][i][0:5]==df['Time'][i+1][0:5]:
      cnt+=1
      end1=i+1
      if cnt==row_num:
        break
    else:
      cnt=1
      break

  end2=len(df['Time'])-1
  start2=end2
  cnt=1
  for i in range(len(df['Time'])-1,-1,-1):
    if df['Time'][i][0:5]==df['Time'][i-1][0:5]:
      cnt+=1
      start2=i-1
      if cnt==row_num:
        break
    else:
      cnt=1
      break

  # print(start1,end1,start2,end2)
  after_avg_list=[]
  column_name=df.columns.tolist()
  df_1=df.iloc[start1:end1+1]
  after_avg_list.append(trends_df_avg(df_1))
  for i in range(end1+1,start2,row_num):
    temp_df=df.iloc[i:i+row_num]
    after_avg_list.append(trends_df_avg(temp_df))
  df_2=df.iloc[start2:end2+1]
  after_avg_list.append(trends_df_avg(df_2))

  return pd.DataFrame(after_avg_list,columns=column_name)


def Trends_df_vertical_merge(Trends_df_list):
  df_merge=Trends_df_list[0]
  for i in range(len(Trends_df_list)-1):
    tmp=[]
    tmp.append(Trends_df_list[i].iloc[len(Trends_df_list[i])-1]['Time'])
    tmp.append(Trends_df_list[i+1].iloc[0]['Time'])
    tmp1_list=tmp[1].split(":")
    tmp1_total_min=int(tmp1_list[0])*60+int(tmp1_list[1])
    tmp0_list=tmp[0].split(":")
    tmp0_total_min=int(tmp0_list[0])*60+int(tmp0_list[1])
    how_many_minute_lost=tmp1_total_min-tmp0_total_min
    lost_minute_list=pd.date_range(tmp[0],periods=how_many_minute_lost,freq="Min").tolist()
    del lost_minute_list[0]
    data_arr=[]
    for j in range(len(lost_minute_list)):
      tmp=[]
      lost_minute_list[j]=lost_minute_list[j].strftime("%H:%M")
      tmp.append(lost_minute_list[j])
      for k in range(len(Trends_df_list[i].columns)-1):
        tmp.append("None")
      data_arr.append(tmp)
    df_compensation=pd.DataFrame(data_arr,columns=Trends_df_list[i].columns).fillna(value="None")
    df_merge=pd.concat([df_merge,df_compensation])
  df_merge=pd.concat([df_merge,Trends_df_list[-1]])
  return df_merge





##################################################################################################################################################################################################################################################################################################################################################
###################################################################________CCO_processing________##########################################################################################################################################################################################################################################################################################################################################################################################################################














def CCO_processing(CCO_filename,CCO_time_gap,row_num):
  CCO_df_list=[]
  for i in range(len(CCO_filename)):
    CCO_data=[]
    cnt=0
    with open(CCO_filename[i], newline='',encoding="utf-8") as csvfile:
      rows = csv.reader(csvfile)
      for row in rows:
        CCO_data.append(row)
      for j in range(len(CCO_data)):
        if CCO_data[j][0]=="日期" and CCO_data[j+1][0]=='':
          cnt=j
          break
          
    CCO_data=CCO_data[cnt:]
    CCO_column_name=[]
    for j in range(len(CCO_data[0])):
      if CCO_data[1][j]!='':
        CCO_column_name.append(CCO_data[0][j]+"("+CCO_data[1][j]+")")
      else:
        CCO_column_name.append(CCO_data[0][j])
    del CCO_data[0]
    del CCO_data[0]
    df=pd.DataFrame(CCO_data,columns=CCO_column_name)
    df= df.drop("日期", axis = 1)
    # df= df.drop("心输出量故障/警示/报警", axis = 1)
    # df= df.drop("血氧测量故障/警示/报警", axis = 1)
    df.rename(columns={'时间': 'Time'}, inplace=True)
    df.fillna(value="None")
    df=CCO_remove_outliers(df)
    df=CCO_time_gap_preprocessing(df,CCO_time_gap)
    df=CCO_avg(df,row_num)
    CCO_df_list.append(df)

  if len(CCO_df_list)==1:
    return CCO_df_list[0]
  else:
    df_vertical_merge=CCO_df_vertical_merge(CCO_df_list)
    return df_vertical_merge

  return df


def CCO_remove_outliers(df):
  range_dic={}
  range_dic['CO(l/min)']=[2.5,7.0]
  range_dic['CI(l/min/m²)']=[1.0,4.0]
  range_dic['SV(ml/b)']=[35.0,100.0]
  range_dic['SVI(ml/b/m²)']=[15.0,50.0]
  range_dic['SVV(%)']=[1.0,30.0]
  range_dic['PRV']=[0.0,0.0]
  range_dic['SVR(dyne-s/cm⁵)']=[0.0,0.0]
  range_dic['SVRI(dyne-s-m²/cm⁵)']=[0.0,0.0]
  range_dic['SvO₂(%)']=[0.0,0.0]
  range_dic['信号质量指数']=[0.0,0.0]
  range_dic['平均脉搏速率(次/分)']=[50.0,150.0]
  range_dic['平均血压(mmHg)']=[60.0,160.0]
  range_dic['CVP(mmHg)']=[0.0,0.0]
  range_dic['心输出量故障/警示/报警']=[0.0,0.0]
  range_dic['血氧测量故障/警示/报警']=[0.0,0.0]

  
  for i in df:
    if i=="Time":
      continue
    data_list=df[i].tolist()
    for j in range(len(data_list)):
      if is_number(data_list[j])==False:
        continue
      data_list[j]=float(data_list[j])
      if data_list[j]>range_dic[i][1] or data_list[j]<range_dic[i][0]:
        data_list[j]="No Data"
    df[i]=data_list
  return df

def CCO_time_gap_preprocessing(df,CCO_time_gap):
  for i in range(len(df['Time'])):
    if(len(df['Time'][i]))>8:
      df['Time'][i]=df['Time'][i][2:]
      df['Time'][i]="0"+df['Time'][i]
  # df['Time'] = pd.to_datetime(df['Time'])
  CCO_time_gap=CCO_time_gap.split(" ")
  time_gap=CCO_time_gap[1]
  time_gap=time_gap.split(":")
  time_gap_to_second=(int(time_gap[2])+int(time_gap[1])*60+int(time_gap[0])*3600)
  if CCO_time_gap[0]=='ADD':
    for j in range(len(df['Time'])):
      df['Time'][j]=pd.Timestamp(df['Time'][j])
      df['Time'][j]=(df['Time'][j]+timedelta(seconds=time_gap_to_second))
      df['Time'][j]=df['Time'][j].strftime("%H:%M:%S")
  elif CCO_time_gap[0]=='SUB':
    for j in range(len(df['Time'])):
      df['Time'][j]=pd.Timestamp(df['Time'][j])
      df['Time'][j]=(df['Time'][j]+timedelta(seconds=-time_gap_to_second))
      df['Time'][j]=df['Time'][j].strftime("%H:%M:%S")
  return df

def CCO_df_avg(df):
  one_row_list=[]
  for i in df:
    if i=="Time":
      now_time_list=(df[i].tolist())[0].split(":")
      now_time=now_time_list[0]+":"+now_time_list[1]
      one_row_list.append(now_time)
      continue
    temp_list=df[i].tolist()
    one_row_list.append(mean(temp_list))

  return one_row_list

def CCO_avg(df,row_num):
  start1=0
  end1=0
  cnt=1
  for i in range(len(df['Time'])):
    # print(df['Time'][i][0:5])
    
    if df['Time'][i][0:5]==df['Time'][i+1][0:5]:
      cnt+=1
      end1=i+1
      if cnt==row_num:
        break
    else:
      cnt=1
      break

  end2=len(df['Time'])-1
  start2=end2
  cnt=1
  for i in range(len(df['Time'])-1,-1,-1):
    if df['Time'][i][0:5]==df['Time'][i-1][0:5]:
      cnt+=1
      start2=i-1
      if cnt==row_num:
        break
    else:
      cnt=1
      break

  # print(start1,end1,start2,end2)
  after_avg_list=[]
  column_name=df.columns.tolist()
  df_1=df.iloc[start1:end1+1]
  after_avg_list.append(CCO_df_avg(df_1))
  for i in range(end1+1,start2,row_num):
    temp_df=df.iloc[i:i+row_num]
    after_avg_list.append(CCO_df_avg(temp_df))
  df_2=df.iloc[start2:end2+1]
  after_avg_list.append(CCO_df_avg(df_2))

  return pd.DataFrame(after_avg_list,columns=column_name)

def CCO_df_vertical_merge(CCO_df_list):
  df_merge=CCO_df_list[0]
  for i in range(len(CCO_df_list)-1):
    tmp=[]
    tmp.append(CCO_df_list[i].iloc[len(CCO_df_list[i])-1]['Time'])
    tmp.append(CCO_df_list[i+1].iloc[0]['Time'])
    tmp1_list=tmp[1].split(":")
    tmp1_total_min=int(tmp1_list[0])*60+int(tmp1_list[1])
    tmp0_list=tmp[0].split(":")
    tmp0_total_min=int(tmp0_list[0])*60+int(tmp0_list[1])
    how_many_minute_lost=tmp1_total_min-tmp0_total_min
    lost_minute_list=pd.date_range(tmp[0],periods=how_many_minute_lost,freq="Min").tolist()
    del lost_minute_list[0]
    data_arr=[]
    for j in range(len(lost_minute_list)):
      tmp=[]
      lost_minute_list[j]=lost_minute_list[j].strftime("%H:%M")
      tmp.append(lost_minute_list[j])
      for k in range(len(CCO_df_list[i].columns)-1):
        tmp.append("None")
      data_arr.append(tmp)
    df_compensation=pd.DataFrame(data_arr,columns=CCO_df_list[i].columns).fillna(value="None")
    df_merge=pd.concat([df_merge,df_compensation])
  df_merge=pd.concat([df_merge,CCO_df_list[-1]])
  return df_merge



##################################################################################################################################################################################################################################################################################################################################################
###################################################################________ANI_processing________##########################################################################################################################################################################################################################################################################################################################################################################################################################

def ANI_processing(ANI_filename,ANI_time_gap,row_num):
  ANI_df_list=[]
  for i in range(len(ANI_filename)):
    ANI_data=[]
    f=open(ANI_filename[i])
    line=f.readline()
    line=f.readline()
    line=f.readline()
    line=f.readline()
    while(line):
      line=line.rstrip().split('\t')
      line.append("False")
      ANI_data.append(line)
      line=f.readline()
    f.close()
    df=pd.DataFrame(ANI_data)
    df= df.drop(1, axis = 1)
    df.columns=['Time','Energy','ANI','ANImean','Events','Time_duplicated']
    time_duplicated=[]
    for j in range(len(df['Time'])-1):
      if df['Time'][j]==df['Time'][j+1]:
        df['Time_duplicated'][j]="True"
      else:
        df['Time_duplicated'][j]="False"
    filt=(df['Time_duplicated']=='False')
    df=(df.loc[filt])
    df=df.drop('Time_duplicated',axis=1)
    df=df.reset_index()
    df=df.drop('index',axis=1)
    df=ANI_lost_time_processing(df)
    df=ANI_remove_outlier(df)
    df=ANI_time_gap_preprocessing(df,ANI_time_gap)
    df=ANI_avg(df,row_num)
    for j in range(len(df['Events'])):
      if df['Events'][j]>0.0:
        df['Events'][j]=1.0
    ANI_df_list.append(df)

  if len(ANI_df_list)==1:
    return ANI_df_list[0]
  else:
    df_vertical_merge=ANI_df_vertical_merge(ANI_df_list)
    return df_vertical_merge
    

def ANI_lost_time_processing(df):
  start_time=df['Time'][0]
  end_time=df['Time'][len(df['Time'])-1]
  start_time_list=start_time.split(":")
  start_time_to_second=int(start_time_list[0])*3600+int(start_time_list[1])*60+int(start_time_list[2])
  end_time_list=end_time.split(":")
  end_time_to_second=int(end_time_list[0])*3600+int(end_time_list[1])*60+int(end_time_list[2])
  how_many_second_lost=end_time_to_second-start_time_to_second
  lost_second_list=pd.date_range(start_time,periods=how_many_second_lost+1,freq='S').tolist()
  data_arr=[]
  for i in range(len(lost_second_list)):
    tmp=[]
    lost_second_list[i]=lost_second_list[i].strftime("%H:%M:%S")
    tmp.append(lost_second_list[i])
    for j in range(len(df.columns.tolist())-1):
      tmp.append("None")
    data_arr.append(tmp)
  df_full_time=pd.DataFrame(data_arr,columns=df.columns)
  cnt=0
  for i in range(len(df_full_time['Time'])):
    if df_full_time['Time'][i]==df['Time'][cnt]:
      df_full_time.iloc[i]=df.iloc[cnt]
      cnt+=1
  return df_full_time

def ANI_remove_outlier(df):
  for i in df:
    if i=="Time":
      continue
    data_list=df[i].tolist()
    for j in range(len(data_list)):
      if is_number(data_list[j])==False:
        continue
      data_list[j]=float(data_list[j])
    df[i]=data_list
  return df

def ANI_time_gap_preprocessing(df,ANI_time_gap):
  for i in range(len(df['Time'])):
    if(len(df['Time'][i]))>8:
      df['Time'][i]=df['Time'][i][2:]
      df['Time'][i]="0"+df['Time'][i]
  # df['Time'] = pd.to_datetime(df['Time'])
  ANI_time_gap=ANI_time_gap.split(" ")
  time_gap=ANI_time_gap[1]
  time_gap=time_gap.split(":")
  time_gap_to_second=(int(time_gap[2])+int(time_gap[1])*60+int(time_gap[0])*3600)
  if ANI_time_gap[0]=='ADD':
    for j in range(len(df['Time'])):
      df['Time'][j]=pd.Timestamp(df['Time'][j])
      df['Time'][j]=(df['Time'][j]+timedelta(seconds=time_gap_to_second))
      df['Time'][j]=df['Time'][j].strftime("%H:%M:%S")
  elif ANI_time_gap[0]=='SUB':
    for j in range(len(df['Time'])):
      df['Time'][j]=pd.Timestamp(df['Time'][j])
      df['Time'][j]=(df['Time'][j]+timedelta(seconds=-time_gap_to_second))
      df['Time'][j]=df['Time'][j].strftime("%H:%M:%S")
  return df


def ANI_df_avg(df):
  one_row_list=[]
  for i in df:
    if i=="Time":
      now_time_list=(df[i].tolist())[0].split(":")
      now_time=now_time_list[0]+":"+now_time_list[1]
      one_row_list.append(now_time)
      continue
    temp_list=df[i].tolist()
    one_row_list.append(mean(temp_list))

  return one_row_list

def ANI_avg(df,row_num):
  start1=0
  end1=0
  cnt=1
  for i in range(len(df['Time'])):
    # print(df['Time'][i][0:5])
    
    if df['Time'][i][0:5]==df['Time'][i+1][0:5]:
      cnt+=1
      end1=i+1
      if cnt==row_num:
        break
    else:
      cnt=1
      break

  end2=len(df['Time'])-1
  start2=end2
  cnt=1
  for i in range(len(df['Time'])-1,-1,-1):
    if df['Time'][i][0:5]==df['Time'][i-1][0:5]:
      cnt+=1
      start2=i-1
      if cnt==row_num:
        break
    else:
      cnt=1
      break

  # print(start1,end1,start2,end2)
  after_avg_list=[]
  column_name=df.columns.tolist()
  df_1=df.iloc[start1:end1+1]
  after_avg_list.append(ANI_df_avg(df_1))
  for i in range(end1+1,start2,row_num):
    temp_df=df.iloc[i:i+row_num]
    after_avg_list.append(ANI_df_avg(temp_df))
  df_2=df.iloc[start2:end2+1]
  after_avg_list.append(ANI_df_avg(df_2))

  return pd.DataFrame(after_avg_list,columns=column_name)

def ANI_df_vertical_merge(ANI_df_list):
  df_merge=ANI_df_list[0]
  for i in range(len(ANI_df_list)-1):
    tmp=[]
    tmp.append(ANI_df_list[i].iloc[len(ANI_df_list[i])-1]['Time'])
    tmp.append(ANI_df_list[i+1].iloc[0]['Time'])
    tmp1_list=tmp[1].split(":")
    tmp1_total_min=int(tmp1_list[0])*60+int(tmp1_list[1])
    tmp0_list=tmp[0].split(":")
    tmp0_total_min=int(tmp0_list[0])*60+int(tmp0_list[1])
    how_many_minute_lost=tmp1_total_min-tmp0_total_min
    lost_minute_list=pd.date_range(tmp[0],periods=how_many_minute_lost,freq="Min").tolist()
    del lost_minute_list[0]
    data_arr=[]
    for j in range(len(lost_minute_list)):
      tmp=[]
      lost_minute_list[j]=lost_minute_list[j].strftime("%H:%M")
      tmp.append(lost_minute_list[j])
      for k in range(len(ANI_df_list[i].columns)-1):
        tmp.append("None")
      data_arr.append(tmp)
    df_compensation=pd.DataFrame(data_arr,columns=ANI_df_list[i].columns).fillna(value="None")
    df_merge=pd.concat([df_merge,df_compensation])
  df_merge=pd.concat([df_merge,ANI_df_list[-1]])
  return df_merge

##################################################################################################################################################################################################################################################################################################################################################
##################################################################________Main_block________##############################################################################################################################################################################################################################################################################################################################################################################################################################
Trends_filename,CCO_filename,ANI_filename,CCO_time_gap,ANI_time_gap,SerialNumber_003,patient_name=read_time_gap_record_data("time_gap_record.xlsx")
for i in range(len(Trends_filename)):
  print(f"Serial number is: {SerialNumber_003[i]},patient name is {patient_name[i]}")
  Trends_df=Trends_processing(Trends_filename[i],2)
  Trends_df.to_csv(f"./Trends_csv/{SerialNumber_003[i]}_trends.csv",index=False,encoding='utf-8-sig')
  print(f"{SerialNumber_003[i]}({patient_name[i]}) trends data processing completed")
  CCO_df=CCO_processing(CCO_filename[i],CCO_time_gap[i],3)
  CCO_df.to_csv(f"./CCO_csv/{SerialNumber_003[i]}_CCO.csv",index=False,encoding='utf-8-sig')
  print(f"{SerialNumber_003[i]}({patient_name[i]}) CCO data processing completed")
  ANI_df=ANI_processing(ANI_filename[i],ANI_time_gap[i],60)
  ANI_df.to_csv(f"./ANI_csv/{SerialNumber_003[i]}_ANI.csv",index=False,encoding='utf-8-sig')
  print(f"{SerialNumber_003[i]}({patient_name[i]}) ANI data processing completed")
  df_merge=pd.merge(left=Trends_df,right=CCO_df,how='outer',on='Time',indicator=False,sort=True).fillna(value="None")
  df_merge=pd.merge(left=df_merge,right=ANI_df,how='outer',on='Time',indicator=False,sort=True).fillna(value="None")
  print("Data merging...")
  print("Data transfer to CSV...")
  df_merge.to_csv(f"./{SerialNumber_003[i]}/{SerialNumber_003[i]}_combine.csv",index=False,encoding='utf-8-sig')
  df_merge.to_csv(f"./trends_CCO_ANI_combine_per_patient/{SerialNumber_003[i]}_combine.csv",index=False,encoding='utf-8-sig')
  print(f"Done! ({i+1}/{len(Trends_filename)})\n")



