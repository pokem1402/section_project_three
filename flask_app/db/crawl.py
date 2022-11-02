from flask_app.db.data import DataReader

def train_test_split(df, criteria = None):
    if criteria is None:
        criteria = '2022-06-01'
    train = df.loc[df.index < criteria].copy()
    test = df.loc[df.index >= criteria].copy()
    return train, test
    

def data_crawling():
    """
        데이터 베이스 초기화
    """
    DataReader.initDB(check_only=False)

    """
        Crawling all features and storing raw features to mysql local server
    """
    df = DataReader.get_collect_all()
    DataReader.execute_import_df(df, "finance")

    """
    Storing train and test data to mysql local server
    """
    x_train, x_test = train_test_split(df)
    DataReader.execute_import_df(x_train, "finance_train_x")
    DataReader.execute_import_df(x_test, "finance_test_x")
 

    df_group = DataReader.make_target_group(df)
    
    
    y_train_3, y_test_3 = train_test_split(df_group[0])
    y_train_7, y_test_7 = train_test_split(df_group[1])
    y_train_14, y_test_14 = train_test_split(df_group[2])
    y_train_21, y_test_21 = train_test_split(df_group[3])
    y_train_28, y_test_28 = train_test_split(df_group[4])
    
    DataReader.execute_import_df(y_train_3, "finance_train_y_3")
    DataReader.execute_import_df(y_test_3, "finance_test_y_3")
    DataReader.execute_import_df(y_train_7, "finance_train_y_7")
    DataReader.execute_import_df(y_test_7, "finance_test_y_7")
    DataReader.execute_import_df(y_train_14, "finance_train_y_14")
    DataReader.execute_import_df(y_test_14, "finance_test_y_14")
    DataReader.execute_import_df(y_train_21, "finance_train_y_21")
    DataReader.execute_import_df(y_test_21, "finance_test_y_21")
    DataReader.execute_import_df(y_train_28, "finance_train_y_28")
    DataReader.execute_import_df(y_test_28, "finance_test_y_28")
    
def loadData():
    df = DataReader.execute_export_db("finance")
    x_train = DataReader.execute_export_db("finance_train_x")
    x_test = DataReader.execute_export_db("finance_test_x")
    y_train_3 = DataReader.execute_export_db("finance_train_y_3")
    y_test_3 = DataReader.execute_export_db("finance_test_y_3")
    y_train_7 = DataReader.execute_export_db("finance_train_y_7")
    y_test_7 = DataReader.execute_export_db("finance_test_y_7")
    y_train_14 = DataReader.execute_export_db("finance_train_y_14")
    y_test_14 = DataReader.execute_export_db("finance_test_y_14")
    y_train_21 = DataReader.execute_export_db("finance_train_y_21")
    y_test_21 = DataReader.execute_export_db("finance_test_y_21")
    y_train_28 = DataReader.execute_export_db("finance_train_y_28")
    y_test_28 = DataReader.execute_export_db("finance_test_y_28")
    
    return {"df":df, 'x_train':x_train, "x_test":x_test,
            'y_train_3':y_train_3, 'y_test_3':y_test_3,
            'y_train_7':y_train_7, 'y_test_7':y_test_7,
            'y_train_14':y_train_14, 'y_test_14':y_test_14,
            'y_train_21':y_train_21, 'y_test_21':y_test_21,
            'y_train_28':y_train_28, 'y_test_28':y_test_28}