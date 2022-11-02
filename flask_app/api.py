from flask import Blueprint, request
import os, json, pickle
import pandas as pd
from flask_app.db.data import DataReader

api_bp = Blueprint('api', __name__)

FILE_PATH = os.path.join(os.getcwd(), "flask_app", "model")
"""
    parameter
    
    1. period  
        default : 3
        Description : 자산 예측 기간 (ex: 3일 선택 시 3일 뒤의 자산 가격 예측) 
        Available : 3, 7, 14, 21, 28
    2. today
        Description : 예측 날짜 (ex : 예측하고 싶은 날)
        Available : 2022-02-01 ~ 2022-10-29
        
    return
    
    1. weight :
        Description : Kospi , S&P 500, gold 권장 투자 비율 (합계 1)
        
"""
@api_bp.route('/', methods=['GET'])
def get_ratio():
    
    parameter_dict = request.args.to_dict()
    
    if "period" not in parameter_dict.keys():
        return "Insert predction period", 400

    period = int(parameter_dict['period'])


    if period not in [3, 7, 14, 21, 28]:
        return f"{period} days are not predictable. 3, 7, 14, 21, and 28 are only available", 400
    
    if "day" not in parameter_dict.keys():
        return "Insert date to predict", 400

    day = pd.to_datetime(parameter_dict['day'])
    
    if (pd.to_datetime("2022-02-01") > day ) or (pd.to_datetime("2022-10-30") < day):
        return "Insert date between 2022-02-01 and 2022-10-30", 400
    
    with open(os.path.join(FILE_PATH, f"softmax_weights_{period}.pickle"), 'rb') as f:
        weights = pickle.load(f, encoding="utf-8")
    try:
        weight = weights.loc[day]
    except KeyError as e:
        day = pd.to_datetime(day) - pd.Timedelta("1 days")
        weight = weights.loc[day]
    
    
    result = {"weights":weight.to_dict(),
              "day" : day}
    
    return result, 200
    