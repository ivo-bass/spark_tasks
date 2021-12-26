from dataclasses import dataclass


@dataclass
class Outputs:
    checkin: str
    review_text_elite_reviews: str
    review_text_best_businesses: str
    review_text_worst_businesses: str
    review_text_most_useful: str
    review_text_count_reviews: str
    tip: str
    elite_user: str
    full_review: str
