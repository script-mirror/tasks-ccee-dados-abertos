from typing import Dict, Type
from TasksInterface import TasksInterface
from middle.utils import Constants
from app.tasks import Cvu

constants = Constants()
PRODUCT_MAPPING: Dict[str, Type[TasksInterface]] = {
    "cvu": Cvu,
}