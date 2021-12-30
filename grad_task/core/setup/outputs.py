from dataclasses import dataclass


@dataclass
class Outputs:
    """This class holds all output paths as strings
    :arg checkin (str): path to write files
    :arg review_text_elite_reviews (str): path to write files
    :arg review_text_best_businesses (str): path to write files
    :arg review_text_worst_businesses (str): path to write files
    :arg review_text_most_useful (str): path to write files
    :arg review_text_count_reviews (str): path to write files
    :arg tip (str): path to write files
    :arg elite_user (str): path to write files
    :arg full_review (str): path to write files
    """
    checkin: str
    review_text_elite_reviews: str
    review_text_best_businesses: str
    review_text_worst_businesses: str
    review_text_most_useful: str
    review_text_count_reviews: str
    tip: str
    elite_user: str
    full_review: str
