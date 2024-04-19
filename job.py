from datetime import datetime
from typing import Any, Union
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from logger import logger


class Job:
    def __init__(
            self,
            task_id: str,
            target: Callable,
            args: Optional[List] = None,
            kwargs: Optional[Dict[str, Any]] = None,
            max_work_time: Optional[int] = None,
            start_at: Optional[datetime] = None,
            depends: Optional[List['Job']] = None,
            tries: int = 0,
    ):
        logger.info(
            f'Job was created with task_id={task_id}, target={target}, args={args}, kwargs={kwargs}, '
            f'max_work_time={max_work_time}, start_at={start_at}, depends={depends}, tries={tries}'
        )
        self.task_id = task_id
        self._target = target
        self._args = args if args else list()
        self._kwargs = kwargs if kwargs else dict()
        self._max_working_time = max_work_time
        self._start_at = start_at
        self.depends = depends if depends else list()
        self._tries = tries

        self._generator = self._target(*self._args, **self._kwargs)

        self._actual_working_time = 0.0
        self._is_done = False
        self._is_stopped = False

    def __repr__(self):
        return (f'Job(task_id={self.task_id}, target={self._target.__name__}, max_work_time={self._max_working_time}, '
                f'start_at={self._start_at}, depends={self.depends}, tries={self._tries})')

    @property
    def tries(self) -> int:
        return self._tries

    @property
    def is_done(self) -> bool:
        return self._is_done

    def _check_dependencies(self) -> bool:
        """
        Проверка зависимостей.

        :return: Результат проверки.
        """
        for dep in self.depends:
            if not dep._is_done:
                logger.info(f'Job {self} is not started yet because of dependency {dep}')
                return False
        logger.info(f'Job {self} can start because all dependencies are done')
        return True

    def _update_actual_working_time(self, start_datetime: datetime) -> None:
        """
        Обновление общего времени выполнения задачи.

        :param start_datetime: Начало последней итерации.
        :return: None.
        """
        end_datetime = datetime.now()
        delta = end_datetime - start_datetime
        self._actual_working_time += delta.total_seconds()
        logger.info(f'Job {self} is working for {self._actual_working_time} seconds')

    def _do_job(self) -> Any:
        """
        Выполнение задачи.

        :return: Результат выполнения.
        """
        logger.info(f'Running job {self}')
        start_time = datetime.now()
        result = next(self._generator)
        self._update_actual_working_time(start_time)
        return result

    def _check_start_at(self) -> bool:
        """
        Проверка отложенного старта задачи.

        :return: Результат проверки.
        """
        if isinstance(self._start_at, datetime):
            if datetime.now() < self._start_at:
                logger.info(f'Job {self} is not started yet because of start_at {self._start_at}')
                return False
        logger.info(f'Job {self} can start now')
        return True

    def _check_working_time(self) -> bool:
        """
        Проверка максимального времени работы.

        :return: Результат проверки.
        """
        if isinstance(self._max_working_time, int):
            return self._actual_working_time < self._max_working_time
        else:
            return True

    def run(self) -> Any:
        """
        Запуск задачи.

        :return: Рузультат выполнения.
        """
        try:
            if self._tries >= 0 and self._check_working_time():
                if self._check_start_at() and self._check_dependencies():
                    return self._do_job()
                else:
                    return None
            else:
                logger.warning(f'Job {self} is over')
                raise StopIteration
        except StopIteration:
            self.stop()
            raise StopIteration(f'StopIteration in job {self}')
        except Exception as exc:  # noqa
            logger.error(f'Exception {exc} in job {self}', exc_info=True)
            self._tries -= 1
        return None

    def pause(self) -> None:
        """
        Приостановка задачи.

        :return: None.
        """
        self._is_stopped = True

    def stop(self) -> None:
        """
        Остановка задачи.

        :return: None.
        """
        self._is_done = True

    def dumps(self) -> Dict[str, Union[str, float, bool]]:
        """
        Получение информации о задаче для записи в конфиг шедулера.

        :return: Информация о состоянии задачи.
        """
        return {
            'task_id': self.task_id,
            'actual_working_time': self._actual_working_time,
            'is_done': self._is_done,
            'is_stopped': self._is_stopped,
            'tries': self._tries,
        }

    def loads(self, task_dumps: Dict[str, Union[float, bool]]) -> None:
        """
        Обновление состояния задачи из конфига шедулера.

        :param task_dumps: Информация о состоянии задачи.
        :return: None.
        """
        self._actual_working_time = task_dumps['actual_working_time']
        self._is_done = task_dumps['is_done']
        self._is_stopped = task_dumps['is_stopped']
        self._tries = task_dumps['tries']
