import numpy as np
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import euclidean, cityblock

def transform_to_embeddings(df):
    embeddings = []
    
    for column in df.columns:
        if df[column].dtype == 'object':
            le = LabelEncoder()
            emb = le.fit_transform(df[column])
            emb = emb.reshape(-1, 1)
            embeddings.append(emb)
        
        elif np.issubdtype(df[column].dtype, np.number):
            scaler = StandardScaler()
            emb = scaler.fit_transform(df[[column]])
            embeddings.append(emb)
        
        elif np.issubdtype(df[column].dtype, np.datetime64):
            emb = np.vstack([df[column].dt.year, df[column].dt.month, df[column].dt.day]).T
            embeddings.append(emb)
    
    return np.hstack(embeddings)



def compare_embeddings(emb1, emb2, method='cosine'):
    """
    Сравнение двух эмбеддингов с использованием различных методов.
    
    Аргументы:
    emb1, emb2 - два массива эмбеддингов для сравнения (должны быть одинаковой длины).
    method - метод для сравнения: 'cosine', 'euclidean', 'manhattan'.
    
    Возвращает:
    Сходство или расстояние между двумя эмбеддингами.
    """
    if method == 'cosine':
        # Косинусное сходство
        similarity = cosine_similarity(emb1.reshape(1, -1), emb2.reshape(1, -1))
        return similarity[0][0]
    
    elif method == 'euclidean':
        # Евклидово расстояние
        return euclidean(emb1, emb2)
    
    elif method == 'manhattan':
        # Манхэттенское (или городское) расстояние
        return cityblock(emb1, emb2)
    
    else:
        raise ValueError(f"Метод {method} не поддерживается. Выберите из 'cosine', 'euclidean', 'manhattan'.")


