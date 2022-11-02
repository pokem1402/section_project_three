
import xgboost
from sklearn.metrics import mean_squared_error
from flask_app.db.data import DataReader
import pandas as pd
import os

class XGBModel:
    
    
    FILE_PATH = f"{os.getcwd()}\\flask_app\\model\\"
    
    def  __init__(self, name):
        
        self.model = self.make_model()
        self.name = name
    
    
    def predict(self, x_test):
        
        columns = DataReader.target_list_name
        x_idx = x_test.index
        
        y_pred = self.model.predict(x_test)
        
        all_day = pd.date_range(x_idx.min()+pd.Timedelta(f"{self.name} days"),
                                x_idx.max()+pd.Timedelta(f"{int(self.name)+1} days"))
        
        y_idx = all_day[all_day.dayofweek != 6][:len(x_idx)]

        return pd.DataFrame(y_pred, columns = columns, index=y_idx).add_suffix(f"_{self.name}")        
    
    
            
    def learning(self, x_train, y_train, eval_set = None, truncate = False):
        
        if not truncate and os.path.exists(f"{self.FILE_PATH}model_{self.name}.ckpt"):
            print("Already Done")
            return self.load_model()
        
        if eval_set == None:
            eval_set = [(x_train, y_train)]
        else:
            eval_set = [(x_train, y_train)] + eval_set
        
        self.model.fit(x_train, y_train, eval_set=eval_set, verbose = 200)
        
        return self
    
    
    
    def scoring(self, x_test, y_test):
            
        return mean_squared_error(y_test, self.model.predict(x_test), squared=False)
        
    def save_model(self):
        self.model.save_model(f"{self.FILE_PATH}model_{self.name}.ckpt")
        
    def load_model(self):
        self.model.load_model(f"{self.FILE_PATH}model_{self.name}.ckpt")
        return self
        
    def make_model(self):

        reg = xgboost.XGBRegressor(base_score=0.5,
                                    booster="gbtree",
                                    n_jobs=-1,
                                    random_state=42,
                                    n_estimators=2000,
                                    early_stopping_rounds=50,
                                    colsample_bytree = 0.7,
                                    max_depth=5,
                                    learning_rate = 0.01)

        return reg
