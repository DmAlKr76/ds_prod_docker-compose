import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

while True:
    try:
        data = pd.read_csv('logs/metric_log.csv')

        fig = plt.figure(figsize=(10, 5))
        sns.histplot(data['absolute_error'], kde=True, color="orange")
        
        plt.savefig('logs/error_distribution.png')
        print('Файл успешно сохранен')
    except:
        print('Произошла ошибка')