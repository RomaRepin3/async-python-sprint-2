from generators import calculate_result
from generators import get_data_from_api
from generators import init_directory
from job import Job
from scheduler import Scheduler
from settings import CITIES


def main():
    schd = Scheduler()

    init_directory_job = Job(task_id='init_directory_job', target=init_directory, args=['results'])
    get_data_from_api_job = Job(
        task_id='get_data_from_api_job',
        target=get_data_from_api,
        args=['results', CITIES],
        depends=[init_directory_job]
    )
    calculate_result_job = Job(
        task_id='calculate_result_job',
        target=calculate_result, args=['results'],
        depends=[init_directory_job, get_data_from_api_job]
    )

    for job in [init_directory_job, get_data_from_api_job, calculate_result_job]:
        schd.schedule(job)
    schd.stop()
    schd.restart()
    schd.stop()


if __name__ == '__main__':
    main()
