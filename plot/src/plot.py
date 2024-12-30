import pandas as pd
import matplotlib.pyplot as plt
import time

while True:
    try:
        metric_log_df = pd.read_csv('./metric/logs/metric_log.csv', sep=',')
        if 'absolute_error' in metric_log_df.columns and not metric_log_df['absolute_error'].empty:
                plt.figure(figsize=(10, 6))
                plt.hist(metric_log_df['absolute_error'], bins=20, color='skyblue', edgecolor='black')
                plt.title('Распределение абсолютных ошибок')
                plt.xlabel('Абсолютные ошибки')
                plt.ylabel('Частота')
                plt.grid(True)
                plt.savefig('logs/error_distribution.png')
                print('Гистограмма обновлена error_distribution')
                plt.close()
        else:
             print("Данные для построения гистограммы еще не пришли")

    except:
        print("Не удалось подключиться к фалу metric log")

    time.sleep(10)