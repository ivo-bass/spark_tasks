from dataclasses import dataclass


@dataclass
class Inputs:
    business: str
    checkin: str
    review: str
    tip: str
    user: str
