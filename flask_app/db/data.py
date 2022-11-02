# data.py

#https://www.kofiabond.or.kr/
import csv, os, datetime, time, requests
from typing import final
from time import strftime
import pandas as pd
import FinanceDataReader as fdr

import pymysql
from sqlalchemy import create_engine



class DataReader:

    OZ = 31.1034768 
    
    # 환율 / kospi 추종 etf / s&p 500 추종 etf / 금 시세
    target_list = ['KS11', "US500", "GC=F"]
    target_list_name = ['KOSPI', "S&P", "GOLD"]

    # 원유 가격 (WIT유, 브렌트유, 천연가스)
    feature_list = ['CL=F', "BZ=F", "NG=F",
                    # 원자재 (백금, 은, 구리, 팔라듐)
                    'PL=F', "SI=F", "HG=F", "PA=F",
                    # 주가 지표 (KOSDAQ, 다우 존스 지수, VIX, 상해 종합 지수, 항셍 지수,
                    #           일본 닛케이 지수, 영국 FTSE100, 프랑스 FCHI 지수, 독일 닥스 지수)
                    'USD/KRW','KQ11', 'DJI', 'VIX', "SSEC", "HSI",
                    "N225", 'FTSE', 'FCHI', 'GDAXI']

    feature_list += target_list

    feature_list_name = ["WIT", "BRENT", "NG",
                         "Pt", "Ag", "Cu", "Pd",
                         'USD/KRW', "KOSDAQ", "DJI", "VIX", "SSEC", "HSI",
                         "N225", "FTSE", "FCHI", "GDAXI"]

    feature_list_name += target_list_name


    # 연준 제공 데이터 리스트
    # 1. 국채 금리 (20년, 10년, 5년, 2년, 1년)
    feature_list_fred = ["DGS20", "DGS10", "DGS5", "DGS2", "DGS1",
                        # 2. 장단기 국채 금리 가격 차이 (10년 - 2년)
                        "T10Y2Y",
                        # 3. 원자재 (니켈, 철광석, 아연, 알루미늄, 납,
                        #           난방유, 호주 석탄, 우라늄, 주석, 유럽 천연가스,
                        #           아시아 LNG, )
                        'PNICKUSDM', 'PIORECRUSDM', 'PZINCUSDM', 'PALUMUSDM', 'PLEADUSDM',
                        'DHOILNYH', 'PCOALAUUSDM', 'PURANUSDM', 'PTINUSDM', 'PNGASEUUSDM',
                        'PNGASJPUSDM',
                        # 4. 농산물 (커피, 해바라기씨유, 밀, 보리, 옥수수,
                        #           고무, 팜유, 올리브유, 목화, 설탕)
                        'PCOFFOTMUSDM', 'PSUNOUSDM', 'PWHEAMTUSDM', 'PBARLUSDM', 'PMAIZMTUSDM',
                        'PRUBBUSDM', 'PPOILUSDM', 'POLVOILUSDM', 'PCOTTINDUSDM', 'PSUGAISAUSDM',
                        # 5. 육류 및 부산품 (소고기, 가금류, 돈육, 가죽, 굵은 울,
                        #                   가는 울)
                        'PBEEFUSDM', 'PPOULTUSDM', 'PPORKUSDM', 'PHIDEUSDM', 'PWOOLCUSDM',
                        'PWOOLFUSDM',
                        # 6. 수산물 (생선, 새우)
                        'PSALMUSDM', 'PSHRIUSDM',
                        # 7. 가격 지수 (에너지 지수, 금속 자원 지수, 농업 원자재 지수, 산업 재료 지수, 음식 및 음료 지수,
                        #               유럽 지역 소비자 물가 지수)
                        'PNRGINDEXM', 'PMETAINDEXM', 'PRAWMINDEXM', 'PINDUINDEXM', 'PFANDBINDEXM',
                        'EA19CPALTT01GYM',
                        # 8. 주가 지수 (나스닥)
                        'NASDAQCOM',
                        # 9. 미국 금융 지표 (통화 공급량, 주간 실업 수당 청구 건수, 소비자 심리지수, 주택 판매 지수,
                        #               High-Yield 채권 스프레드)
                        "M2SL", "ICSA", "UMCSENT", "HSN1F", "UNRATE",
                        "BAMLH0A0HYM2"]
    feature_list_fred_name =["DGS20", "DGS10", "DGS5", "DGS2", "DGS1", "T10Y2Y",
                             "Ni", "Fe", "Zn", "Al", "Pb",
                             "Heat", "Coal", "U", "Sn", "Euro NG", "Asia LNG",
                             "Coffee", "SUNFLOWEROIL","WHEAT", "Barley", "Corn",
                             "Rubber", "Palm", "Olive", "Cotton", "Sugar",
                             "Beef", "Poultry", "Pork", "Hides", "Wool_Coarse", "Wool_Fine",
                             "Fish", "Shirimp",
                             "Energy_idx", "Metal_idx", "Arg_idx", "Ind_mate_idx", "Food_Bev_idx", "EU_CPI",
                             "NASDAQ",
                             "M2", "ICSA", "UMCSENT", "HSN1F", "UNRATE", "HY"]

    # in date / real
    TARGET_PERIOD = zip([3, 6, 12, 18, 24], [3, 7, 14, 21, 28])

    START_DATE = '2004-01-01'
    END_DATE = (datetime.date.today()-datetime.timedelta(1)).strftime("%Y-%m-%d")
    
    def __init__(self):
        
        self.FILE_EXISTS = False        
        if os.path.exists(f"os.getcwd()\\db\\{self.FILE_NAME}"):
            FILE_EXISTS = True
            
        assert len(self.target_list) == len(self.target_list_name)
        assert len(self.feature_list) == len(self.feature_list_name)
        assert len(self.feature_list_fred) == len(self.feature_list_fred_name)
        

    def __init__(self, START_DATE):
        
        self.START_DATE=START_DATE
        
        self.__init__()
        
    
    @classmethod
    def get_collect_from_kb(cls, start_date = None, end_date=None):
        
        """
            한국 은행 DB로부터 데이터 수집
            service_code : [기간, [세부 코드:[option code]], [이름]]
        """
        feature_list = {
            # 국채 일일 금리 : 20년, 10년, 5년, 1년
            "817Y002": ['D', ['010220000', '010210000', '010200001', '010190000'],
                        ['KRB_20Y', 'KRB_10Y', 'KRB_5Y', 'KRB_1Y']],
            # 소비자 월별 물가 지수
            "901Y009": ['M', ['0'], ["KR_CPI"]],
            # 주택 매매 가격 지수
            "901Y062": ["M", ['P63A'], ["KR_HPI"]],
            # 주택 전세 가격 지수
            "901Y063": ["M", ['P64A'], ["KR_HJI"]],
            # # 실업급여 수급 실적 : 2017년 이후부터 자료가 있어서 제거
            # "901Y084": ["M", ['167A:P', "167A:A"], ["KR_UBP", "KR_UBT"]]
        }
        if start_date == None:
            start_date = cls.START_DATE
        if end_date == None:
            end_date = cls.END_DATE
        df = []
        
        for service_code, items in feature_list.items():
            for idx, stats_code in enumerate(items[1]):
               df.append(cls._collect_kb(service_code, stats_code, items[2][idx], date_type = items[0],
                                         start_date = start_date, end_date = end_date)) 
                
        
        return (pd.DataFrame(df).T)
            
        
    @classmethod
    def _collect_kb(cls, service_code, stats_code, name,
                    start_date,
                    end_date, date_type = "D"):
        

        
        key = "G87CNHI36CP7T4PU1PS1"  # TODO : goto enviroment variable
        service_name = "StatisticSearch"
        start= 1
        end = 5000  # maximum
        
        
        start_date = start_date.replace("-", "")
        end_date = end_date.replace("-", "")
        
        if date_type == "M":
            start_date = start_date[:-2]
            end_date = end_date[:-2]        
        
        category = ''
        
        if len(stats_code.split(":")) > 1:
            t = stats_code.split(':')
            category = f"/{t[1]}"
            stats_code = t[0]
        
        url = f"https://ecos.bok.or.kr/api/{service_name}/{key}/" + \
              f"json/kr/{start}/{end}/{service_code}/{date_type}/{start_date}/{end_date}/{stats_code}{category}"""

        time.sleep(1)
        resp = requests.get(url)
        j = resp.json()
        
        try:
            data = j[service_name]
        except KeyError:
            print(service_code, j)
            return
            
        
        list_total_count, rows = data['list_total_count'], data['row']
        
        date, value = [], []
        
        for row in rows:
            date.append(row['TIME'] + ('01' if date_type == "M" else ""))
            value.append(row['DATA_VALUE'])
        
        return pd.Series(value, index = pd.to_datetime(date), dtype=float, name=name)        
    

    @classmethod
    def get_collect_from_fred(cls, start_date = None, end_date = None):
        
        if start_date is None:
            start_date = cls.START_DATE
        if end_date is None:
            end_date = cls.END_DATE
        
        feature_list = cls.feature_list_fred
        feature_list_name = cls.feature_list_fred_name
       
        l = len(feature_list)
        df = pd.DataFrame()
        for i in range(int((l+4)/5)):
            features = "FRED:"+",".join(feature_list[i*5:(i+1)*5])        
            temp = fdr.DataReader(features, start = start_date, end=end_date,data_source="fred")
            temp.columns = feature_list_name[i*5:(i+1)*5]
            df = pd.concat([df, temp], axis=1)
  
        return df
    
    @classmethod
    def get_collect_features(cls, start_date = None, end_date = None):
        
        if start_date is None:
            start_date = cls.START_DATE
        if end_date is None:
            end_date = cls.END_DATE
        
        feature_list = cls.feature_list
        feature_list_name = cls.feature_list_name
        
        # df = []
        # for feature, name in zip(feature_list, feature_list_name):
        #     df.append(fdr.DataReader(feature, start=start_date, end=cls.END_DATE)['Close'].rename(name))
        features = ",".join(feature_list)
        df = fdr.DataReader(features, start = start_date, end = end_date)
        df.columns = feature_list_name
        
        return df

    @classmethod
    def make_target_group(cls, df):
        
        _df = df[cls.target_list_name].copy()
        
        group = []
        
        for period, real in cls.TARGET_PERIOD:
            
            group.append(_df.shift(-period).add_suffix(f"_{real}"))
            
        return group
    
    @classmethod
    def get_collect_all(cls, start_date = None, end_date = None):
            
        from_kb   = cls.get_collect_from_kb(start_date, end_date)
        from_invs = cls.get_collect_features(start_date, end_date)
        from_fred = cls.get_collect_from_fred(start_date, end_date)
        
        # concatenate all data       
        df = pd.concat([from_fred, from_kb, from_invs], axis=1) #.fillna(method="ffill").fillna(method="bfill")
                
        # add feature about date
        df['day'] = df.index.dayofweek
        sunday_idx = df.loc[df['day'] == 6].index
        df['quarter'] = df.index.quarter
        df['month'] = df.index.month
        df['year'] = df.index.year  
        df['dayofyear'] = df.index.dayofyear   
                
        df = df.drop(index = sunday_idx).interpolate().fillna(method='bfill')
        
        return df
    
    
    # mysql
    user_name = 'root'
    password = '9987'
    charset = 'utf8'
    DATABASE_NAME = "PROJECT3"
    
    
    @classmethod
    def initDB(cls, user_name = None, password = None, check_only = False):
        
        auth = cls.make_auth(user_name, password)
        if check_only:
            conn = None
            try:
                conn = cls.connect(auth)
                print("Connection test is success")
            except Exception as e:
                print("Connection test is failed")
                print (e)
            finally:
                conn.close()
        else:
            cls.execute_manifulate(f"DROP DATABASE IF EXISTS {cls.DATABASE_NAME}", auth=auth)
            cls.execute_manifulate(f"CREATE DATABASE IF NOT EXISTS {cls.DATABASE_NAME}", auth=auth)

    @classmethod
    def make_auth(cls, user_name, password):
        if (user_name is not None) and (password is not None):
            user_name, password = cls.user_name, cls.password

        auth = {"user_name":cls.user_name,
                "password":cls.password}

        return auth

    @classmethod
    def execute_import_df(cls, df, table_name, user_name = None, password = None):
        auth = cls.make_auth(user_name, password)
        
        return cls._import_df_to_db(df, table_name, auth)
    
    @classmethod
    def _import_df_to_db(cls, df, table_name, auth, if_exists='replace'):
        
        conn = None
        
        try:
            conn = cls.make_connect(auth)
            results = df.to_sql(name=table_name, con=conn, if_exists=if_exists)

        except Exception as e:
            print(e)
    
        finally:
            conn.close()
        
        return results


    @classmethod
    def execute_export_db(cls, table_name, user_name = None, password = None):
        auth = cls.make_auth(user_name, password)
        
        return cls._export_table_to_df(table_name, auth)

    @classmethod
    def _export_table_to_df(cls, table_name, auth):
        
        conn = None
        df = None
        try:
            conn = cls.make_connect(auth)
            df = pd.read_sql_table(table_name, conn)
        except Exception as e:
            print(e)
        finally:
            conn.close()
        
        return df.set_index(df['index']).drop(columns=['index'])


    @classmethod
    def execute_manifulate(cls, sql, auth):

        conn = None
        try:
            conn = cls.connect(auth)
            curs = conn.cursor()
            
            curs.execute(sql)
            conn.commit()

        except Exception as e:
            print(e)
        
        finally:
            conn.close()                
    
    @classmethod
    def execute_fetchdata(cls, sql, auth):
        
        data = None
        conn = cls.connect(auth)
        try:

            curs = conn.cursor()
            
            curs.execute(sql)
            data = curs.fetchall()
        
        except Exception as e:
            print(e)
        
        finally:
            conn.close()
            
        return data   
     
    @classmethod
    def connect(cls, auth):
        return pymysql.connect(host='localhost', user=auth["user_name"],
                        password = auth['password'], charset=cls.charset)

    @classmethod
    def make_connect(cls, auth):
        
        db_connection_str = f"mysql+pymysql://{auth['user_name']}:{auth['password']}@localhost/{cls.DATABASE_NAME}"
        db_connection = create_engine(db_connection_str)
        return db_connection.connect()
        
    