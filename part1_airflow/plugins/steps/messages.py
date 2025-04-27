# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook
import os

def send_telegram_failure_message(context):
    
    hook = TelegramHook(token='7987871770:AAHovyJvZ3y_NOTVNyMBl9ZQrBHu6pF5BlM', chat_id='-4718420948')
    
    dag = context['dag'].dag_id  # используем .dag_id для получения имени DAG
    run_id = context['run_id']
    task_id = context['task_instance_key_str']
    
    message = f'Исполнение DAG {dag} с task_id={task_id} и run_id={run_id} прошло с ошибкой!'
    
    hook.send_message({
        'chat_id': chat_id,
        'text': message
    })

def send_telegram_success_message(context):
    hook = TelegramHook(token='7987871770:AAHovyJvZ3y_NOTVNyMBl9ZQrBHu6pF5BlM', chat_id='-4718420948')
    
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'
    
    hook.send_message({
        'chat_id': '-4718420948',
        'text': message
    })