import pika
import json
import math
import csv


#Рассчет абсолютной ошибки
def count_abs_err(y_true, y_pred):
    return math.fabs(y_true - y_pred)

FIELDNAMES = ['id', 'y_true', 'y_pred', 'absolute_error']

#Запись логов в файл
def write_log():
    list_result = []
    for id in result_dict:
        if len(result_dict[id]) == 2:
            abs_err = count_abs_err(result_dict[id]['y_true'], result_dict[id]['y_pred'])
            temp_dict = {'id': id, 'y_true':result_dict[id]['y_true'], 'y_pred':result_dict[id]['y_pred'],'absolute_error':abs_err}
            print(f'подготовлены данные для записи в файл {temp_dict}')
            list_result.append(temp_dict)

    with open('logs/metric_log.csv', mode='w', newline='') as file:
        csv_writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        csv_writer.writeheader()
        csv_writer.writerows(list_result)
        print("данные записаны в файл metric_log.csv")
    

result_dict = dict()

while True:
    try:

        # Создаём подключение к серверу на локальном хосте
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()

        # Объявляем очередь y_true
        channel.queue_declare(queue='y_true')

        # Объявляем очередь y_pred
        channel.queue_declare(queue='y_pred')

        

        # Создаём функцию callback для обработки данных из очереди
        def callback(ch, method, properties, body):
            queue = method.routing_key
            value = json.loads(body)

            print(f'Из очереди {queue} получено значение {value['body']} c Id {value['id']}')
            
            if value['id'] in result_dict:
                result_dict[value['id']][queue] = value['body']
            else:
                result_dict[value['id']] = {}
                result_dict[value['id']][queue] = value['body']

            write_log()

        # Извлекаем сообщение из очереди y_true
        channel.basic_consume(
            queue='y_true',
            on_message_callback=callback,
            auto_ack=True
        )
        # Извлекаем сообщение из очереди y_pred
        channel.basic_consume(
            queue='y_pred',
            on_message_callback=callback,
            auto_ack=True
        )

    
        # Запускаем режим ожидания прихода сообщений
        print('...Ожидание сообщений, для выхода нажмите CTRL+C')
        channel.start_consuming()
 
    
    except:
        print('Не удалось подключиться к очереди')

