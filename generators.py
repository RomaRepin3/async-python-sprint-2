from json import dumps
from json import loads
from typing import List

from logger import logger


def init_directory(results_path: str) -> None:
    """
    Создание директории и файлов.

    :param results_path: Название для директории выгрузки результатов и расчётов.
    :return: None.
    """
    from os import mkdir

    try:
        logger.info(f'Creating directory {results_path}')
        mkdir(results_path)
    except FileExistsError:
        logger.warning(f'Directory {results_path} already exists')
    data_file = open(f'{results_path}/data.json', 'w')
    data_file.write(dumps(dict(), indent=4))
    data_file.close()
    result_file = open(f'{results_path}/result.json', 'w')
    result_file.write(dumps(dict(), indent=4))
    result_file.close()
    logger.info(f'Directory {results_path} was created')
    yield


def get_data_from_api(results_path: str, urls: List[str]) -> None:
    """
    Выгрузка данных из API.

    :param results_path: Название для директории выгрузки результатов и расчётов.
    :param urls: Ссылки на API.
    :return: None.
    """
    from http import HTTPStatus
    from urllib.request import urlopen

    while urls:
        url = urls[0]
        try:
            logger.info(f'Get data from {url}')
            with urlopen(url) as response:
                resp_body = response.read().decode('utf-8')
                data = loads(resp_body)
            if response.status != HTTPStatus.OK:
                raise Exception(
                    "Error during execute request. {}: {}".format(
                        resp_body.status, resp_body.reason
                    )
                )
            with open(f'{results_path}/data.json', 'r') as file:
                data_from_file = file.read()
            summary_data = loads(data_from_file) if data_from_file else dict()
            summary_data[data['geo_object']['locality']['name']] = data
            with open(f'{results_path}/data.json', 'w') as file:
                file.write(dumps(summary_data, indent=4))
            logger.info(f'Get data from {url} was successful')
        except Exception as ex:
            logger.error(ex)
            raise Exception(f'Unexpected error: {ex}')
        urls.pop(0)
        yield


def calculate_result(results_path: str) -> None:
    """
    Расчёт средних показателей по дням.

    :param results_path: Название для директории выгрузки результатов и расчётов.
    :return: None.
    """

    with open(f'{results_path}/data.json', 'r') as file:
        data = loads(file.read())

    for item in data.values():
        logger.info(f'Calculate result for {item["geo_object"]["locality"]["name"]}')
        middle_temp = 0.0
        middle_pressure_mm = 0.0
        middle_humidity = 0.0

        result = dict()

        for forecast in item['forecasts']:
            for hour in forecast['hours']:
                middle_temp += hour['temp'] if hour.get('temp') else 0.0
                middle_pressure_mm += hour['pressure_mm'] if hour.get('pressure_mm') else 0.0
                middle_humidity += hour['humidity'] if hour.get('humidity') else 0.0

            middle_temp /= len(forecast['hours']) if len(forecast['hours']) > 0 else 1
            middle_pressure_mm /= len(forecast['hours']) if len(forecast['hours']) > 0 else 1
            middle_humidity /= len(forecast['hours']) if len(forecast['hours']) > 0 else 1

            result[forecast['date']] = {
                'temp': middle_temp,
                'pressure_mm': middle_pressure_mm,
                'humidity': middle_humidity,
            }

        with open(f'{results_path}/result.json', 'r') as file:
            result_from_file = file.read()
        summary_result = loads(result_from_file) if result_from_file else dict()
        summary_result[item['geo_object']['locality']['name']] = result
        with open(f'{results_path}/result.json', 'w') as file:
            file.write(dumps(summary_result, indent=4))
        logger.info(f'Calculate result for {item["geo_object"]["locality"]["name"]} was successful')
        yield
