from dataclasses import dataclass


@dataclass
class Configurator:
    spark: dict
    logging: dict
    inputs: dict
    outputs: dict
