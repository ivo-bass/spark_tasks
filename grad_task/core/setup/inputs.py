from dataclasses import dataclass


@dataclass
class Inputs:
    """This class holds input paths as strings
    :arg business (str): path to 'business' dataset
    :arg checkin (str): path to 'checkin' dataset
    :arg review (str): path to 'review' dataset
    :arg tip (str): path to 'tip' dataset
    :arg user (str): path to 'user' dataset
    """
    business: str
    checkin: str
    review: str
    tip: str
    user: str
