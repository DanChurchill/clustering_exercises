import pandas as pd
import numpy as np
import env
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler



def get_connection(db, user=env.user, host=env.host, password=env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def get_mallcustomer_data():
    '''
    Reads in all fields from the customers table in the mall_customers schema from data.codeup.com
    
    parameters: None
    
    returns: a single Pandas DataFrame with the index set to the primary customer_id field
    '''
    df = pd.read_sql('SELECT * FROM customers;', get_connection('mall_customers'))
    return df.set_index('customer_id')

def summarize(df):
    print('DataFrame head: \n')
    print(df.head(3))
    print('--------------')
    print('Shape:   ', df.shape)
    print('---------------')
    print('Info:    \n')
    df.info()
    print('---------------')
    print(df.describe())
    print('---------------')
    print(nulls_by_col(df))
    print('---------------')
    print(nulls_by_row(df))

def detect_outliers(df): 
    cols = df.select_dtypes(include=np.number).columns.tolist()
    
    for col in cols:
        q1, q3 = df[col].quantile([.25, .75]) # get range

        iqr = q3 - q1   # calculate interquartile range

        upper_bound = q3 + 1.5 * iqr   # get upper bound
        lower_bound = q1 - 1.5 * iqr   # get lower bound

        upper_df = df[df[col] > upper_bound]
        lower_df = df[df[col] < lower_bound]
        
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]

    return df


def split_mall(df):
    # separate into 80% train/validate and test data
    train_validate, test = train_test_split(df, test_size=.2, random_state=333)

    # further separate the train/validate data into train and validate
    train, validate = train_test_split(train_validate, 
                                        test_size=.25, 
                                        random_state=333)

    return train, validate, test

def mall_dummies(df):
    dummies = pd.get_dummies(df['gender'], drop_first=True)
    df = pd.concat([df, dummies], axis=1)
    df.drop(columns=['gender'], inplace=True)
    return df

def mall_scaler(train, validate, test):
    columns=train.columns
    scaler = MinMaxScaler()
    train[columns] = scaler.fit_transform(train[columns])
    validate[columns] = scaler.transform(validate[columns])
    test[columns] = scaler.transform(test[columns])

    return train,validate,test
