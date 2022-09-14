import os
from xml.etree.ElementPath import prepare_parent
import pandas as pd
import numpy as np
import env
from sklearn.model_selection import train_test_split

def get_connection(db, user=env.user, host=env.host, password=env.password):
    '''
    function to generate a url for querying the codeup database
    accepts a database name (string) and requires an env.py file with 
    username, host, and password.

    Returns an url as a string  
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'



def get_zillow():
    """
    Retrieve locally cached data .csv file for the zillow dataset
    If no locally cached file is present retrieve the data from the codeup database server
    Keyword arguments: none
    Returns: DataFrame

    """
    
    filename = "zillow.csv"

    # if file is available locally, read it
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    
    else:
    # if file not available locally, acquire data from SQL database
    # and write it as csv locally for future use 
        df = pd.read_sql('''
                            SELECT parcelid, basementsqft, bathroomcnt as bathrooms, bedroomcnt as bedrooms,
                                    calculatedbathnbr, finishedfloor1squarefeet, calculatedfinishedsquarefeet, finishedsquarefeet12, 
                                    finishedsquarefeet13, finishedsquarefeet15, finishedsquarefeet50, finishedsquarefeet6, fips, fireplacecnt,
                                    fullbathcnt, garagecarcnt, garagetotalsqft as garagesqft, hashottuborspa, latitude, 
                                    longitude, lotsizesquarefeet as lotsize, poolcnt, poolsizesum,
                                    pooltypeid10, pooltypeid2, pooltypeid7, propertycountylandusecode, propertyzoningdesc,
                                    rawcensustractandblock, regionidcity, regionidcounty, 
                                    regionidneighborhood, regionidzip, roomcnt, threequarterbathnbr,
                                    unitcnt, yardbuildingsqft17, yardbuildingsqft26, yearbuilt, 
                                    numberofstories, fireplaceflag, structuretaxvaluedollarcnt,
                                    taxvaluedollarcnt as tax_value, assessmentyear, landtaxvaluedollarcnt,
                                    taxamount, taxdelinquencyflag, taxdelinquencyyear, censustractandblock, logerror, transactiondate,
                                    airconditioningdesc, architecturalstyledesc, buildingclassdesc,
                                    heatingorsystemdesc, storydesc, propertylandusedesc, typeconstructiondesc
                            FROM properties_2017
                            JOIN predictions_2017
                            USING (parcelid)
                            LEFT JOIN airconditioningtype
                            USING (airconditioningtypeid)
                            LEFT JOIN architecturalstyletype
                            USING (architecturalstyletypeid)
                            LEFT JOIN buildingclasstype
                            USING (buildingclasstypeid)
                            LEFT JOIN heatingorsystemtype
                            USING (heatingorsystemtypeid)
                            LEFT JOIN storytype
                            USING (storytypeid)
                            LEFT JOIN propertylandusetype
                            USING (propertylandusetypeid)
                            LEFT JOIN typeconstructiontype
                            USING (typeconstructiontypeid)
                            ORDER BY transactiondate
                    ''', get_connection('zillow'))

    # remove duplicate properties (keeping row with latest transaction date)
    df = df[~df.duplicated(subset=['parcelid'],keep='last')]
    
    # Write that dataframe to disk for later. This cached file will prevent repeated large queries to the database server.
    df.to_csv(filename, index=False)
    return df

def single_family(df):
    df=df[df.propertylandusedesc == 'Single Family Residential']
    df=df[~df['unitcnt'].isin([0,2,3])]
    df=df[df.bedrooms != 0]
    return df

def handle_missing_values(df, prop_required_column=.5, prop_required_row=.75):
    threshold = int(round(prop_required_column * len(df.index), 0))
    df = df.dropna(axis=1, thresh = threshold)
    threshold = int(round(prop_required_row * len(df.columns), 0))
    df = df.dropna(axis=0, thresh = threshold)
    return df

def nulls_by_col(df):
    num_missing = df.isnull().sum()
    prcnt_miss = num_missing / df.shape[0] * 100
    
    cols_missing = pd.DataFrame({'num_rows_missing' : num_missing,
                                 'percent_rows_missing' : prcnt_miss})
    return cols_missing

def nulls_by_row(df):
    num_missing = df.isnull().sum(axis=1)
    prcnt_miss = num_missing / df.shape[1] * 100
    
    rows_missing = pd.DataFrame({'num_cols_missing': num_missing, 'percent_cols_missing' : prcnt_miss})
    rows_missing = rows_missing.reset_index().groupby(['num_cols_missing',
                'percent_cols_missing']).count().reset_index().rename(columns={'index' : 'count'})
    return rows_missing

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

def wrangle_zillow():
    # get data
    df=get_zillow()

    # filter for single family
    df=single_family(df)

    # summarize
    summarize(df)

    # # show nulls
    # print(nulls_by_col(df))
    # print('-----------------')
    # print(nulls_by_row(df))

    # handle nulls
    df = handle_missing_values(df, prop_required_column=.5, prop_required_row=.75)

    df = remove_outliers(df)

    
    
    return df

def my_split(df):
    '''
    Separates a dataframe into train, validate, and test datasets

    Keyword arguments:
    df: a dataframe containing multiple rows
    

    Returns:
    three dataframes who's length is 60%, 20%, and 20% of the length of the original dataframe       
    '''

    # separate into 80% train/validate and test data
    train_validate, test = train_test_split(df, test_size=.2, random_state=333)

    # further separate the train/validate data into train and validate
    train, validate = train_test_split(train_validate, 
                                        test_size=.25, 
                                        random_state=333)

    return train, validate, test

# handle outliers
def remove_outliers(df):
    ''' remove outliers from a list of columns in a dataframe 
        accepts a dataframe and a list of columns from which to remove outliers
        returns a dataframe with outliers removed
    '''
    col_list = [ 'bathrooms', 'bedrooms', 'calculatedbathnbr',
       'calculatedfinishedsquarefeet', 'finishedsquarefeet12', 'fips',
       'fullbathcnt', 'lotsize',
       
       'regionidcity', 'regionidcounty', 'regionidzip', 'roomcnt', 'yearbuilt',
       'structuretaxvaluedollarcnt', 'tax_value', 
       'landtaxvaluedollarcnt', 'taxamount', 'censustractandblock', 'logerror',
       
       'county']
    
    for col in col_list:

        q1, q3 = df[col].quantile([.25, .75]) # get range
        
        iqr = q3 - q1   # calculate interquartile range
        
        upper_bound = q3 + 1.5 * iqr   # get upper bound
        lower_bound = q1 - 1.5 * iqr   # get lower bound

        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]

        return df
