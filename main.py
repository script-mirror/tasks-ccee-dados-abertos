import sys
from middle.utils import setup_logger
from app.mapping import PRODUCT_MAPPING
from app.TasksInterface import TasksInterface

logger = setup_logger()


def task_handler(nome: str):
    product_handler: TasksInterface | None = PRODUCT_MAPPING[nome]
    if not product_handler:
        logger.error(f"Produto {nome} nao encontrado no mapeamento")
        raise ValueError("Produto nao mapeado")
    product_handler = PRODUCT_MAPPING[nome]()
    result = product_handler.run_workflow()
    return result

if __name__ == "__main__":
    logger.info("Iniciando aplicacao CCEE dados abertos")
    if len(sys.argv) >= 1:
        nome = sys.argv[1]
        task_handler(nome)
        logger.info(f"nome: {nome}")
    else:
        raise ValueError("nome nao fornecido corretamente")

    logger.info("Aplicacao finalizada com sucesso")