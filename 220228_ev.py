from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tqdm import tqdm
import argparse
import math
import os
import pandas as pd
import requests
import schedule

class EVmonitor:
    def __init__(self) -> None:

        self.__args = self.getArgs()

        self.__url_info = 'http://apis.data.go.kr/B552584/EvCharger/getChargerInfo'
        self.__url_status = 'http://apis.data.go.kr/B552584/EvCharger/getChargerStatus'
        self.__params_info = {
            'serviceKey' : self.__args.key, 
            'pageNo' : '1', 
            'numOfRows' : '10'
        }
        self.__params_status = {
            'serviceKey' : self.__args.key, 
            'pageNo' : '1', 
            'numOfRows' : '9000'
        }

        self.__n_rows = 9000

        req_init = requests.get(self.__url_info, params=self.__params_info)
        soup = BeautifulSoup(req_init.text, 'html.parser')

        self.__pages = list(range(1, (math.ceil(int(soup.find('totalcount').text.strip())/self.__n_rows))+1))
        totalcount = soup.find('totalcount').text.strip()
        print(f'========== 전체 개수: {totalcount} ==========')

        self.run1()

        schedule.every(self.__args.interval).minutes.do(self.run2)
        while True:
            schedule.run_pending()

    def getArgs(self):
        parser = argparse.ArgumentParser(description='Get EvCharger info')
        parser.add_argument('--key', required=True, help='apikey')
        parser.add_argument('--dir', required=True, help='directory to save csv file')
        parser.add_argument('--interval', type=int, required=True, help='update interval (minute)')
        args = parser.parse_args()
        return args
    
    def getInfo(self):

        self.__params_info['numOfRows'] = self.__n_rows

        dict_info = {}
        tags = ['statnm', 'statid', 'chgerid', 'chgertype', 'addr', 'usetime',
                'busiid', 'bnm', 'businm', 'busicall', 'stat', 'statupddt', 
                'lasttsdt', 'lasttedt', 'nowtsdt', 'output', 'method', 'zcode', 
                'parkingfree', 'note', 'limityn', 'limitdetail']
        
        # 데이터 불러온 시각
        now = datetime.now()
        krnow = now.astimezone(timezone(timedelta(hours=9)))     # 한국 시간대로 변경
        self.__init_time = krnow.strftime('%y%m%d-%H%M%S')
        
        # 페이지 넘겨가며 데이터 불러오기 (한 페이지에 모든 결과 불러올 수 없음. API 활용가이드 참고.)
        for pagenum in tqdm(self.__pages):
            self.__params_info['pageNo'] = pagenum
            req = requests.get(self.__url_info, params=self.__params_info)
            soup = BeautifulSoup(req.text, 'html.parser')

            for t in tags:
                tlist = soup.find_all(t)
                tlist = [e.text.strip() for e in tlist]

                if pagenum == 1:
                    dict_info[t] = tlist
                else:
                    dict_info[t] += tlist
        
        self.__init_info = pd.DataFrame(dict_info)

        return self.__init_info
    
    def saveInitialInfo(self):
        # 구글 colab+드라이브에서 실행하는 경우 권한 필요
        if os.getcwd() == '/content':
            from google.colab import drive
            drive.mount('/content/drive')
        
        fname = self.__init_time + '.csv'
        savepath = os.path.join(self.__args.dir, fname)
        Path(self.__args.dir).mkdir(parents=True, exist_ok=True)
        
        self.__init_info.to_csv(savepath, encoding='utf-8-sig', index = False)

        print(f'========== 초기 정보 저장: {savepath} ==========')
    
    def updateStatus(self):

        self.__updated_info = self.__init_info.copy()

        dict_stat = {}
        tags_stat = ['busiid', 'statid', 'chgerid', 'stat', 'statupddt', 'lasttsdt', 'lasttedt', 'nowtsdt']

        # 데이터 업데이트 시각
        now = datetime.now()
        krnow = now.astimezone(timezone(timedelta(hours=9)))     # 한국 시간대로 변경
        self.__update_time = krnow.strftime('%y%m%d-%H%M%S')

        # 데이터 불러오기
        req = requests.get(self.__url_status, params=self.__params_status)
        soup = BeautifulSoup(req.text, 'html.parser')
        for t in tags_stat:
            tlist = soup.find_all(t)
            tlist = [e.text.strip() for e in tlist]
            dict_stat[t] = tlist
        df_stat = pd.DataFrame(dict_stat)

        self.__n_updates = len(df_stat)
        self.__updated_rows = []

        # 업데이트한 정보 기존 정보에 합치기
        for i in range(len(df_stat)):
            is_statid = self.__updated_info['statid'] == df_stat.iloc[i, 1]
            is_chgerid = self.__updated_info['chgerid'] == df_stat.iloc[i, 2]
            row_idx = self.__updated_info[is_statid & is_chgerid].index[0]
            self.__updated_info.loc[row_idx, 'stat'] = df_stat.iloc[i, 3]
            self.__updated_info.loc[row_idx, 'statupddt'] = df_stat.iloc[i, 4]
            self.__updated_info.loc[row_idx, 'lasttsdt'] = df_stat.iloc[i, 5]
            self.__updated_info.loc[row_idx, 'lasttedt'] = df_stat.iloc[i, 6]
            self.__updated_info.loc[row_idx, 'nowtsdt'] = df_stat.iloc[i, 7]
            self.__updated_rows.append(row_idx)
        
        # 업데이트 정보 출력
        print(
            f'업데이트 개수: {self.__n_updates}', 
            f'업데이트한 행 번호: {self.__updated_rows}', 
            sep='\n'
        )

        return self.__updated_info
    
    def saveUpdatedInfo(self):
        # 구글 colab+드라이브에서 실행하는 경우 권한 필요
        if os.getcwd() == '/content':
            from google.colab import drive
            drive.mount('/content/drive')
        
        fname = self.__update_time + '.csv'
        savepath = os.path.join(self.__args.dir, fname)
        Path(self.__args.dir).mkdir(parents=True, exist_ok=True)
        
        self.__updated_info.to_csv(savepath, encoding='utf-8-sig', index = False)

        print(f'========== 업데이트 저장: {savepath} ==========')
    
    def run1(self):
        self.getInfo()
        self.saveInitialInfo()
    
    def run2(self):
        self.updateStatus()
        self.saveUpdatedInfo()

def main():
    EVmonitor()

if __name__ == '__main__':
    main()