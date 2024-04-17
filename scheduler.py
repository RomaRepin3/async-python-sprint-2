from json import dumps
from json import loads
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from job import Job
from logger import logger


class Scheduler:
    CONFIG_FILE_NAME = 'scheduler_info.json'

    def __init__(self, pool_size: int = 10):
        logger.info(f'Scheduler was created with pool_size={pool_size}')
        self._pool_size = pool_size
        self._queue: List[Job] = list()
        self._ended_tasks = list()

        self._has_stop = False

    def schedule(self, task: Job) -> None:
        """
        Добавление задачи в очередь выполнения.

        :param task: Объект задачи.
        :return: None.
        """
        if len(self._queue) <= self._pool_size:
            logger.info(f'Scheduler was scheduled with task={task}')
            self._queue.append(task)
        else:
            logger.warning(f'Can"t adding task {task}: queue is over')

    def _get_task(self) -> Optional[Job]:
        """
        Получение задачи из очереди.

        :return: None.
        """
        if not self._queue:
            logger.warning('Can"t get task: queue is empty')
            return None
        task = self._queue.pop(0)
        logger.info(f'Getting task {task} from queue')
        return task

    def _add_task_dump_to_ended_task(self, task: Job) -> None:
        """
        Добавление информации о состоянии задачи к списку завершенных задач.

        :param task: Объект задачи.
        :return: None.
        """
        self._ended_tasks.append(task.dumps())

    def _process_task(self, task: Optional[Job]) -> Any:
        """
        Выполнение итерации задачи.

        :param task: Объект задачи.
        :return: Результат выполнения задачи.
        """
        if task is None:
            logger.warning('Can"t process task: task is None')
            return None
        try:
            logger.info(f'Processing task {task}')
            result = task.run()
            if not task.is_done:
                logger.info(f'Task {task} is not done')
                self.schedule(task)
            else:
                logger.info(f'Task {task} is done')
                self._ended_tasks.append(task.dumps())
            return result
        except StopIteration:
            logger.warning(f'StopIteration in task {task}')
            self._ended_tasks.append(task.dumps())
            return None

    def run(self) -> None:
        """
        Запуск шедулера.

        :return: None.
        """
        if not self._has_stop:
            while True:
                task = self._get_task()
                if task:
                    self._process_task(task)
                else:
                    break
        else:
            logger.warning('Scheduler stopped early and needs to be restarted')

    @staticmethod
    def _mapping_task_dumps(task_dumps: Dict[str, Union[str, float, bool]]) -> Dict[str, Union[float, bool]]:
        """
        Преобразование информации о состоянии задачи для записи в конфиг шедулера.

        :param task_dumps: Информация о состоянии задачи.
        :return: Преобразованная информация о состоянии задачи.
        """
        return {
            'actual_working_time': task_dumps['actual_working_time'],
            'is_done': task_dumps['is_done'],
            'is_stopped': task_dumps['is_stopped'],
            'tries': task_dumps['tries'],
        }

    def restart(self) -> None:
        """
        Обновление состояния задач шедулера из конфига.

        :return: None.
        """
        logger.info('Restarting scheduler')
        with open(Scheduler.CONFIG_FILE_NAME, 'r') as file:
            scheduler_info = loads(file.read())
        for task in self._queue:
            if task.task_id in scheduler_info.keys():
                task.loads(scheduler_info[task.task_id])
        self._has_stop = False
        logger.info('Scheduler restarted')
        self.run()

    def stop(self) -> None:
        """
        Остановка шедулера и запись информации о состоянии задач в конфиг.

        :return: None.
        """
        logger.info('Stopping scheduler')
        tasks = [task.dumps() for task in self._queue]
        scheduler_info = dict()

        for task in tasks:
            scheduler_info[task['task_id']] = self._mapping_task_dumps(task)
        for task in self._ended_tasks:
            scheduler_info[task['task_id']] = self._mapping_task_dumps(task)
        with open(Scheduler.CONFIG_FILE_NAME, 'w') as file:
            file.write(dumps(scheduler_info, indent=4, ))
        self._has_stop = True
        logger.info('Scheduler stopped')
