from abc import ABC
from middle.utils import setup_logger
from middle.utils import Constants
logger = setup_logger()
constants = Constants()

class TasksInterface(ABC):

    def __init__(self):
        pass

    def run_workflow(self):
        pass

    def run_process(self):
        pass
