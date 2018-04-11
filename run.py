import logging
import logging.config
import csv
import asyncio
from aiohttp import web, ClientSession


logger = logging.getLogger('bot')


USERS_FNAME = 'users.csv'
TOKENS_FNAME = 'tokes.csv'
HOST = '0.0.0.0'
PORT = 8080
READ_TIMEOUT = 60
CONN_TIMEOUT = 60


def read_data(fname):
    data = {}
    with open(fname, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            data[row[0]] = row[1]
    return data


async def push(request):
    data = await request.json()
    logger.info('send data {} '.format(data))

    tokens = read_data(TOKENS_FNAME)
    user_ids = set(read_data(USERS_FNAME).values())
    token = tokens[data['bot_name']]
    to = data.get('to', 'all')
    subscriber_ids = user_ids if to == 'all' else {to, }
    if not subscriber_ids.issubset(user_ids):
        raise ValueError('invalid user id')

    text = '*{}*\n\n'
    text += '{}'
    text = text.format(data['title'], data['msg'])

    async with ClientSession(raise_for_status=True, read_timeout=READ_TIMEOUT, conn_timeout=CONN_TIMEOUT) as session:
        done, pending = await asyncio.wait([session.post('https://api.telegram.org/bot{}/sendmessage'.format(token),
                                                         json={'chat_id': s_id, 'text': text, 'parse_mode': 'Markdown'})
                                            for s_id in subscriber_ids])
        for f in done:
            logger.error(f.exception())

    logger.info('data is sent')
    return web.Response()


if __name__ == '__main__':
    app = web.Application()
    app.router.add_post('/push', push)

    logging.config.dictConfig({'version': 1,
                              'disable_existing_loggers': False,
                              'formatters': {
                                  'common': {
                                      'format': '[%(asctime)s %(name)s %(levelname)s] %(message)s'
                                  },
                              },
                               'filters': {
                                   'nomsg': {
                                       '()': 'log.NoMsgFilter',
                                       },
                               },
                              'handlers': {
                                  'common': {
                                      'class': 'logging.StreamHandler',
                                      'level': 'DEBUG',
                                      'filters': ['nomsg'],
                                      'formatter': 'common',
                                  },
                              },
                              'loggers': {
                                  '': {
                                      'handlers': ['common'],
                                      'level': 'DEBUG'
                                  },
                              }
                          })

    web.run_app(app, host=HOST, port=PORT)
