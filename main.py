#!/usr/bin/env python3
# coding: utf-8

# Copyright (c) Latona. All rights reserved.

import asyncio
import logging
import os

import MySQLdb

# RabbitMQ用モジュール
from rabbitmq_client import RabbitmqClient

# JSONロギング用モジュール
from custom_logger import init_logger

SERVICE_NAME = 'register-face-to-guest-table-kube'

# ログ出力用インスタンス
logger = logging.getLogger(__name__)


class OmotebakoDB():
    def __init__(self):
        self.connection = MySQLdb.connect(
            host=os.environ.get('MYSQL_HOST'),
            user=os.environ.get('MYSQL_USER'),
            passwd=os.environ.get('MYSQL_PASSWORD'),
            db='Omotebako',
            charset='utf8')
        self.cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)

    def getAllGuest(self):
        sql = '''
            select 
                guest_id,
                face_id_azure
            from Omotebako.guest;
        '''
        self.cursor.execute(sql)
        return self.cursor.fetchone()

    def getGuestFromAzureID(self, face_id_azure):
        sql = '''
            select 
                guest_id,
                face_id_azure
            from Omotebako.guest
            where face_id_azure="%s";
        ''' % (face_id_azure)
        self.cursor.execute(sql)
        return self.cursor.fetchone()

    def insertNewGuest(self, face_id_azure, face_image_path, gender, age):
        sql2 = ''' 
            insert into Omotebako.guest
            (face_id_azure, face_image_path, gender_by_face, age_by_face) 
            values 
            ("%s", "%s", "%s", "%s"); 
            ''' % (face_id_azure, face_image_path, gender, age)
        self.cursor.execute(sql2)
        self.connection.commit()

    def insertNewGuestID(self, face_image_path):
        sql = ''' 
            insert into Omotebako.guest
            (face_image_path) 
            values 
            ("%s"); 
            ''' % (face_image_path)

    def updateGuest(self, guest_id, face_image_path, face_id_azure):
        sql = ''' 
            update Omotebako.guest
            set face_image_path="%s", face_id_azure="%s"
            where guest_id=%s ; 
            ''' % (face_image_path, face_id_azure, guest_id)
        self.cursor.execute(sql)
        self.connection.commit()


async def main():
    init_logger()

    # RabbitMQ接続情報
    rabbitmq_url = os.environ['RABBITMQ_URL']
    # キューの読み込み元
    queue_origin = os.environ['QUEUE_ORIGIN']

    try:
        mq_client = await RabbitmqClient.create(rabbitmq_url, {queue_origin})
    except Exception as e:
        logger.error({
            'message': 'failed to connect rabbitmq!',
            'error': str(e),
            'queue_origin': queue_origin,
        })
        # 本来 sys.exit を使うべきだが、効かないので
        os._exit(1)

    logger.info('create mq client')

    async for message in mq_client.iterator():
        try:
            async with message.process():
                logger.info({
                    'message': 'message received',
                    'params': message.data
                })
                guest_id = int(message.data.get('guest_id'))
                image = message.data.get('filepath')
                face_id_azure = message.data.get('face_id_azure')

                try:
                    db = OmotebakoDB()
                    db.updateGuest(
                        guest_id=guest_id,
                        face_image_path=image,
                        face_id_azure=face_id_azure
                    )
                except Exception as e:
                    logger.error({
                        'message': 'updateGuest error',
                        'error': str(e),
                    })
                    raise e

                logger.info({
                    'message': 'updateGuest succeeded',
                    'params': {
                      "guest_id":   guest_id,
                      "face_id": face_id_azure,
                    } 
                })

        except Exception as e:
            logger.error({
                'message': 'execute error',
                'error': str(e),
            })
            continue

if __name__ == '__main__':
    asyncio.run(main())