# scripts/fit.py

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from catboost import CatBoostRegressor
import yaml
import os
import joblib

# обучение модели
def fit_model():
	# загрузка гиперпараметров из файла params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    
    # загрузка результата предыдущего шага: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')
    data.drop(columns='building_id', inplace=True)
    
    # обучение модели
    cat_features = data.select_dtypes(include='bool')
    potential_binary_features = cat_features.nunique() == 2

    binary_cat_features = cat_features[potential_binary_features[potential_binary_features].index]
    num_features = data.select_dtypes(['float'])
    rank_features = data.select_dtypes(include=['int'])

    preprocessor = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_cat_features.columns.tolist()),
            ('int', CatBoostEncoder(return_df=False), rank_features.columns.tolist()),
            ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = CatBoostRegressor()

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(data, data[params['target_col']]) 
    
	# сохранение обученной модели в models/fitted_model.pkl
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd) 

if __name__ == '__main__':
	fit_model()