from cmath import nan
from PyQt5 import QtWidgets, QtGui, QtCore
from mainwindowUI import Ui_MainWindow
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
import sys
from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtWidgets import QApplication, QMainWindow
## pyuic5 -x mainwindow.ui -o mainwindowUI.py
class WorkThread(QThread):
    # 自定義訊號物件。引數str就代表這個訊號可以傳一個字串
    trigger = pyqtSignal(str)

    def __init__(self):
        # 初始化函式
        super(WorkThread, self).__init__()

    def run(self):
        #重寫執行緒執行的run函式
        #觸發自定義訊號
        for i in range(20):
            time.sleep(1)
            # 通過自定義訊號把待顯示的字串傳遞給槽函式
            self.trigger.emit(str(i))
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
		# in python3, super(Class, self).xxx = super().xxx
        super(MainWindow, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.setup_control()
        self.patient_data_attribute=['surgery_date','SerialNumber','ResearchSerialNumber','Name','PatientNumber','CCO_time_gap','ANI_time_gap','is_the_data_complete?','have_been_merged']
        self.Trends_filename,self.CCO_filename,self.ANI_filename,self.CCO_time_gap,self.ANI_time_gap,self.SerialNum_003Num,self.patient_name,self.df_time_gap_record,self.have_been_merged_data_filename,self.have_been_merged_data_SerialNum_003Num,self.have_been_merged_data_patient_name=self.read_time_gap_record_data("time_gap_record.xlsx")
        self.ui.start_pushButton.clicked.connect(self.start_click)
        self.ui.have_complete_data_checkBox.clicked.connect(self.have_complete_data_check)
        self.ui.have_been_merged_checkBox.clicked.connect(self.have_been_merged_check)   
        self.ui.data_list_tableWidget.cellClicked.connect(self.data_list_tableWidget_click)  
        self.ui.data_list_tableWidget.setHorizontalHeaderLabels(self.patient_data_attribute)
        self.ui.show_pushButton.clicked.connect(self.show_pushButton_click)
        self.ui.display_combine_data_pushButton.clicked.connect(self.display_combine_data_pushButton_click)
        for i in range(len(self.have_been_merged_data_SerialNum_003Num)):
            item=self.have_been_merged_data_SerialNum_003Num[i]+"  "+self.have_been_merged_data_patient_name[i]
            self.ui.now_show_patient_information_ComboBox.addItem(item)
        self.ui.now_show_patient_information_ComboBox.currentIndexChanged.connect(self.change_now_show_patient_information_ComboBox)
        self.current_patient=self.have_been_merged_data_SerialNum_003Num[0]
    def read_time_gap_record_data(self,fi):
        df=pd.read_excel(fi).fillna(value="None")


        # surgery_date=df['surgery_date'].tolist()
        # for i in range(len(surgery_date)):
        #     if surgery_date[i]!="None":
        #         surgery_date[i]=surgery_date[i].strftime("%Y-%m-%d")
        # df['surgery_date']=surgery_date
        
        SerialNumber=df['SerialNumber'].tolist()
        for i in range(len(SerialNumber)):
            SerialNumber[i]="0"+str(SerialNumber[i])
        df['SerialNumber']=SerialNumber


        PatientNumber=df['PatientNumber'].tolist()
        for i in range(len(PatientNumber)):
            PatientNumber[i]="0"*(8-len(str(PatientNumber[i])))+str(PatientNumber[i])
        df['PatientNumber']=PatientNumber  

        
        filt=(df['is_the_data_complete?']=='y')
        df_filt=(df.loc[filt])


        SerialNum_003Num=[]
        CCO_time_gap=[]
        ANI_time_gap=[]
        patient_name=[]
        have_been_merged_data_filename=[]
        have_been_merged_data_SerialNum_003Num=[]
        have_been_merged_data_patient_name=[]
        for i in range(len(df_filt)):
            tmp=(df_filt.iloc[i]['ResearchSerialNumber'].split("-"))[1]
            SerialNum_003Num.append(df_filt.iloc[i]['SerialNumber']+"_"+tmp)
            CCO_time_gap.append(df_filt.iloc[i]['CCO_time_gap'])
            ANI_time_gap.append(df_filt.iloc[i]['ANI_time_gap'])
            patient_name.append(df_filt.iloc[i]['Name'])
            if df_filt.iloc[i]['have_been_merged']=='y':
                have_been_merged_data_filename.append("./trends_CCO_ANI_combine_per_patient/"+df_filt.iloc[i]['SerialNumber']+"_"+tmp+"_combine.csv")
                have_been_merged_data_SerialNum_003Num.append(df_filt.iloc[i]['SerialNumber']+"_"+tmp)
                have_been_merged_data_patient_name.append(df_filt.iloc[i]['Name'])


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
        self.check_if_all_files_exist(Trends_filename,CCO_filename,ANI_filename)
        return Trends_filename,CCO_filename,ANI_filename,CCO_time_gap,ANI_time_gap,SerialNum_003Num,patient_name,df,have_been_merged_data_filename,have_been_merged_data_SerialNum_003Num,have_been_merged_data_patient_name

    def check_if_all_files_exist(self,Trends_filename,CCO_filename,ANI_filename):
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

    def setup_control(self):
        # TODO
        return
    def start_click(self):
        for i in range(len(self.Trends_filename)):
            self.work_thread=WorkThread()
            print(f"Serial number is: {self.SerialNum_003Num[i]},patient name is {self.patient_name[i]}")
            Trends_df=self.Trends_processing(self.Trends_filename[i],2)
            Trends_df.to_csv(f"./Trends_csv/{self.SerialNum_003Num[i]}_trends.csv",index=False,encoding='utf-8-sig')
            print(f"{self.SerialNum_003Num[i]}({self.patient_name[i]}) trends data processing completed")
            CCO_df=self.CCO_processing(self.CCO_filename[i],self.CCO_time_gap[i],3)
            CCO_df.to_csv(f"./CCO_csv/{self.SerialNum_003Num[i]}_CCO.csv",index=False,encoding='utf-8-sig')
            print(f"{self.SerialNum_003Num[i]}({self.patient_name[i]}) CCO data processing completed")
            ANI_df=self.ANI_processing(self.ANI_filename[i],self.ANI_time_gap[i],60)
            ANI_df.to_csv(f"./ANI_csv/{self.SerialNum_003Num[i]}_ANI.csv",index=False,encoding='utf-8-sig')
            print(f"{self.SerialNum_003Num[i]}({self.patient_name[i]}) ANI data processing completed")
            df_merge=pd.merge(left=Trends_df,right=CCO_df,how='outer',on='Time',indicator=False,sort=True).fillna(value="None")
            df_merge=pd.merge(left=df_merge,right=ANI_df,how='outer',on='Time',indicator=False,sort=True).fillna(value="None")
            print("Data merging...")
            print("Data transfer to CSV...")
            df_merge.to_csv(f"./{self.SerialNum_003Num[i]}/{self.SerialNum_003Num[i]}_combine.csv",index=False,encoding='utf-8-sig')
            df_merge.to_csv(f"./trends_CCO_ANI_combine_per_patient/{self.SerialNum_003Num[i]}_combine.csv",index=False,encoding='utf-8-sig')
            print(f"Done! ({i+1}/{len(self.Trends_filename)})\n")
            for j in range(len(self.df_time_gap_record['Name'])):
                if self.df_time_gap_record.iloc[j]['Name']==self.patient_name[i]:
                    self.df_time_gap_record.iloc[j]['have_been_merged']='y'
        self.df_time_gap_record.to_excel(f"time_gap_record.xlsx",index=False,encoding='utf-8-sig')
            
            
        return


    def have_complete_data_check(self):
        return
    def have_been_merged_check(self):
        return
    def show_pushButton_click(self):
        if self.ui.have_complete_data_checkBox.isChecked():
            filt=(self.df_time_gap_record['is_the_data_complete?']=='y')
            self.df_filt=(self.df_time_gap_record.loc[filt])
            if self.ui.have_been_merged_checkBox.isChecked():
                filt=(self.df_filt['have_been_merged']=='y')
                self.df_filt=(self.df_filt[filt])
        else:
            self.df_filt=self.df_time_gap_record
            if self.ui.have_been_merged_checkBox.isChecked():
                filt=(self.df_filt['have_been_merged']=='y')
                self.df_filt=(self.df_filt[filt])
        self.ui.data_list_tableWidget.setRowCount(len(self.df_filt['Name']))
        self.ui.data_list_tableWidget.setColumnCount(len(self.patient_data_attribute))
        self.ui.data_list_tableWidget.setHorizontalHeaderLabels(self.patient_data_attribute)
        self.ui.data_list_tableWidget.setVerticalHeaderLabels(["1", "2", "3", "4", "5","6","7","8","9"])
        self.data_list=[]
        for i in range(len(self.df_filt['Name'])):
            self.data_list.append(self.df_filt.iloc[i].tolist())
        

        for i, product in enumerate(self.data_list):
            for j, attribute in enumerate(product):                
                self.ui.data_list_tableWidget.setItem(i, j, QtWidgets.QTableWidgetItem(attribute))
        return
    def data_list_tableWidget_click(self):
        return
    def Trends_processing(self,Trends_filename,row_num):
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
    
            df=self.trends_remove_outliers(df)
            df=self.data_avg(df,row_num)
            Trends_df_list.append(df)
        if len(Trends_df_list)==1:
            return Trends_df_list[0]
        else:
            df_vertical_merge=self.data_df_vertical_merge(Trends_df_list)
            return df_vertical_merge


    def trends_remove_outliers(self,df):
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
    def is_number(self,num):
        pattern=re.compile(r'(.*)\.(.*)\.(.*)')
        if pattern.match(num):
            return False
        return num.replace(".","").isdigit()
    def mean(self,lst):
        sum_num=0.0
        int_num=0
        for i in lst:
            if self.is_number(str(i)):
                sum_num+=i
                int_num+=1
        if int_num==0:
            return "None"
        return round(sum_num/int_num,2)
    def data_df_avg(self,df):
        one_row_list=[]
        for i in df:
            if i=="Time":
                now_time_list=(df[i].tolist())[0].split(":")
                now_time=now_time_list[0]+":"+now_time_list[1]
                one_row_list.append(now_time)
                continue
            temp_list=df[i].tolist()
            one_row_list.append(self.mean(temp_list))

        return one_row_list

    def data_avg(self,df,row_num):
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
        after_avg_list.append(self.data_df_avg(df_1))
        for i in range(end1+1,start2,row_num):
            temp_df=df.iloc[i:i+row_num]
            after_avg_list.append(self.data_df_avg(temp_df))
        df_2=df.iloc[start2:end2+1]
        after_avg_list.append(self.data_df_avg(df_2))

        return pd.DataFrame(after_avg_list,columns=column_name)


    def data_df_vertical_merge(self,df_list):
        df_merge=df_list[0]
        for i in range(len(df_list)-1):
            tmp=[]
            tmp.append(df_list[i].iloc[len(df_list[i])-1]['Time'])
            tmp.append(df_list[i+1].iloc[0]['Time'])
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
                for k in range(len(df_list[i].columns)-1):
                    tmp.append("None")
                data_arr.append(tmp)
            df_compensation=pd.DataFrame(data_arr,columns=df_list[i].columns).fillna(value="None")
            df_merge=pd.concat([df_merge,df_compensation])
        df_merge=pd.concat([df_merge,df_list[-1]])
        return df_merge

    def CCO_processing(self,CCO_filename,CCO_time_gap,row_num):
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
            df=self.CCO_remove_outliers(df)
            df=self.time_gap_preprocessing(df,CCO_time_gap)
            df=self.data_avg(df,row_num)
            CCO_df_list.append(df)

        if len(CCO_df_list)==1:
            return CCO_df_list[0]
        else:
            df_vertical_merge=self.data_df_vertical_merge(CCO_df_list)
            return df_vertical_merge



    def CCO_remove_outliers(self,df):
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
                if self.is_number(data_list[j])==False:
                    continue
                data_list[j]=float(data_list[j])
                if data_list[j]>range_dic[i][1] or data_list[j]<range_dic[i][0]:
                    data_list[j]="No Data"
            df[i]=data_list
        return df

    def time_gap_preprocessing(self,df,data_time_gap):
        for i in range(len(df['Time'])):
            if(len(df['Time'][i]))>8:
                df['Time'][i]=df['Time'][i][2:]
                df['Time'][i]="0"+df['Time'][i]
        # df['Time'] = pd.to_datetime(df['Time'])
        data_time_gap=data_time_gap.split(" ")
        time_gap=data_time_gap[1]
        time_gap=time_gap.split(":")
        time_gap_to_second=(int(time_gap[2])+int(time_gap[1])*60+int(time_gap[0])*3600)
        if data_time_gap[0]=='ADD':
            for j in range(len(df['Time'])):
                df['Time'][j]=pd.Timestamp(df['Time'][j])
                df['Time'][j]=(df['Time'][j]+timedelta(seconds=time_gap_to_second))
                df['Time'][j]=df['Time'][j].strftime("%H:%M:%S")
        elif data_time_gap[0]=='SUB':
            for j in range(len(df['Time'])):
                df['Time'][j]=pd.Timestamp(df['Time'][j])
                df['Time'][j]=(df['Time'][j]+timedelta(seconds=-time_gap_to_second))
                df['Time'][j]=df['Time'][j].strftime("%H:%M:%S")
        return df

    def ANI_processing(self,ANI_filename,ANI_time_gap,row_num):
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
            df=self.ANI_lost_time_processing(df)
            df=self.ANI_remove_outlier(df)
            df=self.time_gap_preprocessing(df,ANI_time_gap)
            df=self.data_avg(df,row_num)
            for j in range(len(df['Events'])):
                if df['Events'][j]>0.0:
                    df['Events'][j]=1.0
            ANI_df_list.append(df)

        if len(ANI_df_list)==1:
            return ANI_df_list[0]
        else:
            df_vertical_merge=self.data_df_vertical_merge(ANI_df_list)
            return df_vertical_merge 
    

    def ANI_lost_time_processing(self,df):
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

    def ANI_remove_outlier(self,df):
        for i in df:
            if i=="Time":
                continue
            data_list=df[i].tolist()
            for j in range(len(data_list)):
                if self.is_number(data_list[j])==False:
                    continue
                data_list[j]=float(data_list[j])
            df[i]=data_list
        return df
    def change_now_show_patient_information_ComboBox(self):
        self.current_patient = self.ui.now_show_patient_information_ComboBox.currentText()
        self.current_patient=(self.current_patient.split("  "))[0]
        return
    def display_combine_data_pushButton_click(self):
        self.now_file_name=""
        for i in range(len(self.have_been_merged_data_filename)):
            if self.current_patient in self.have_been_merged_data_filename[i]:
                self.now_file_name=self.have_been_merged_data_filename[i]
        now_patient_data_list=[]
        with open(self.now_file_name, newline='',encoding="utf-8") as csvfile:
            rows = csv.reader(csvfile)
            for row in rows:
                now_patient_data_list.append(row)
            column_name=now_patient_data_list[0]
            del now_patient_data_list[0]
        now_patient_df=pd.DataFrame(now_patient_data_list,columns=column_name)
        now_patient_df=now_patient_df.rename(columns={'\ufeffTime':'Time'})
        self.ui.merged_data_list_tableWidget.setRowCount(len(now_patient_df['Time']))
        self.ui.merged_data_list_tableWidget.setColumnCount(len(column_name))
        self.ui.merged_data_list_tableWidget.setHorizontalHeaderLabels(column_name)
        self.ui.merged_data_list_tableWidget.setVerticalHeaderLabels(["1", "2", "3", "4", "5","6","7","8","9"])
        self.data_list=[]        

        for i, product in enumerate(now_patient_data_list):
            for j, attribute in enumerate(product):                
                self.ui.merged_data_list_tableWidget.setItem(i, j, QtWidgets.QTableWidgetItem(attribute))
        
        return


if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())