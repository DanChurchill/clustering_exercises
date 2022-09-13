import os
import pandas as pd
import numpy as np

def single_family(df):
    df=df[df.propertylandusedesc == 'Single Family Residential']
    df=df[df.unitcnt <= 1]

def handle_missing_values(df, prop_required_column=.5, prop_required_row=.75):
    threshold = int(round(prop_required_column * len(df.index), 0))
    df = df.dropna(axis=1, thresh = threshold)
    threshold = int(round(prop_required_row * len(df.columns), 0))
    df = df.dropna(axis=0, thresh = threshold)
    return df