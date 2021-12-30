from dataclasses import dataclass


@dataclass
class Configurator:
    """This class holds all configurations as dictionaries
    :arg spark (dict): spark configurations
    :arg logging (dict): logging configurations
    :arg inputs (dict): input paths
    :arg outputs (dict): output paths
    """
    spark: dict
    logging: dict
    inputs: dict
    outputs: dict
