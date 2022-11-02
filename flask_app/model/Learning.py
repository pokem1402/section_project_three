from flask_app.db.data import DataReader
from flask_app.db.crawl import loadData
from flask_app.model import XGBModel

def learning():

    data = loadData()
    
    x_train = data['x_train']
    x_test = data['x_test']
    
    y_train_3 = data['y_train_3']
    y_train_7 = data['y_train_7']
    y_train_14 = data['y_train_14']
    y_train_21 = data['y_train_21']
    y_train_28 = data['y_train_28']
    
    y_val_3 = set_validation(data['y_test_3'])
    y_val_7 = set_validation(data['y_test_7'])
    y_val_14 = set_validation(data['y_test_14'])
    y_val_21 = set_validation(data['y_test_21'])
    y_val_28 = set_validation(data['y_test_28'])
    
    x_val = set_validation(x_test)

    print("xgb_model_day_3")
    xgbmodel_3 = XGBModel("3").learning(x_train, y_train_3, eval_set=[(x_val, y_val_3)])
    xgbmodel_3.save_model()

    print("xgb_model_day_7")
    xgbmodel_7 = XGBModel("7").learning(x_train, y_train_7, eval_set=[(x_val, y_val_7)])
    xgbmodel_7.save_model()
    
    print("xgb_model_day_14")
    xgbmodel_14 = XGBModel("14").learning(x_train, y_train_14, eval_set=[(x_val, y_val_14)])
    xgbmodel_14.save_model()
    
    print("xgb_model_day_21")
    xgbmodel_21 = XGBModel("21").learning(x_train, y_train_21, eval_set=[(x_val, y_val_21)])
    xgbmodel_21.save_model()
    
    print("xgb_model_day_28")
    xgbmodel_28 = XGBModel("28").learning(x_train, y_train_28, eval_set=[(x_val, y_val_28)])
    xgbmodel_28.save_model()
    

def set_validation(df, criteria = '2022-08-01'):
    return df[df.index < criteria]


def load_model():
    xgbmodel_3 = XGBModel(3).load_model()
    xgbmodel_7 = XGBModel(7).load_model()
    xgbmodel_14 = XGBModel(14).load_model()
    xgbmodel_21 = XGBModel(21).load_model()
    xgbmodel_28 = XGBModel(28).load_model()
    return {"model_3" : xgbmodel_3,
            "model_7" : xgbmodel_7,
            "model_14": xgbmodel_14,
            "model_21": xgbmodel_21,
            "model_28": xgbmodel_28}
    
def pred_save():
    models = load_model()
    x_test = DataReader.execute_export_db("finance_test_x")
    predict_2022_3 = model['xgbmodel_3'].predict(x_test)
    predict_2022_7 = model['xgbmodel_7'].predict(x_test)
    predict_2022_14 = model['xgbmodel_14'].predict(x_test)
    predict_2022_21 = model['xgbmodel_21'].predict(x_test)
    predict_2022_28 = model['xgbmodel_28'].predict(x_test)
    
    predict_28 = predict_2022_28[predict_2022_28.index <= DataReader.END_DATE]
    DataReader.execute_import_df(predict_28, "predict_28")
    
    predict_21 = predict_2022_21[predict_2022_21.index <= DataReader.END_DATE]
    DataReader.execute_import_df(predict_21, "predict_21")
    
    predict_14 =predict_2022_14[predict_2022_14.index <= DataReader.END_DATE]
    DataReader.execute_import_df(predict_14, "predict_14")
    
    predict_7 = predict_2022_7[predict_2022_7.index  <= DataReader.END_DATE]
    DataReader.execute_import_df(predict_7, "predict_7")
    
    predict_3 = predict_2022_3[predict_2022_3.index  <= DataReader.END_DATE]
    DataReader.execute_import_df(predict_3, "predict_3")
    