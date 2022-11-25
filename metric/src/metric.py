import pika
import json
import pandas as pd
import csv
 
try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
   
    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')

    # Инициализируем таблицу из файла
    log_df = pd.read_csv('logs/metric_log.csv') 
    
    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        # Считываем данные из очереди
        message = json.loads(body)
        message_id = message['id']
        message_body = message['body'] 
        
        # Логируем в labels_log.txt
        answer_string = f'Из очереди {method.routing_key} получено значение {message_body}'
        with open('./logs/labels_log.txt', 'a') as log:
            log.write(answer_string +'\n')
        print(answer_string)
        
        # Заносим тело сообщения в таблицу
        log_df.loc[message_id, method.routing_key] = message_body
        # Если получены y_true и y_pred
        if not any(log_df.loc[message_id, ['y_true', 'y_pred']].isna()):
            # Вычисляем абсолютную ошибку модели для данного наблюдения
            y_true = log_df.loc[message_id, 'y_true'] 
            y_pred = log_df.loc[message_id, 'y_pred']
            absolute_error = abs(y_true - y_pred)

            # Заносим ошибку в таблицу
            log_df.loc[message_id, 'absolute_error'] = absolute_error

            # Логируем в таблицу       
            with open('logs/metric_log.csv', 'a', newline='') as csvlog:
                writer = csv.writer(csvlog, delimiter=',')
                writer.writerow([message_id, y_true, y_pred, absolute_error])  
        
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
except Exception as error:
    print('Не удалось подключиться к очереди:', error)