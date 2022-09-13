import os
import pandas as pd
import numpy as np
import env

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