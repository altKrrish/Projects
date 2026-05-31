import re
import datetime


def validate_question(question):
    """
    Validate user question before SQL generation.

    Returns:
        []                          -> valid
        [issue_dict]                -> clarification needed
    """

    detected_issues = []

    question_padded = f" {question.lower()} "

    # --------------------------------------------------
    # Ambiguous Terms
    # --------------------------------------------------
    known_terms = {
        "volume": [
            "Sold Quantity",
            "Cancelled Quantity",
            "Quantity"
        ],

        "date": [
            "OrderDate",
            "DispatchDate",
            "DeliveryDate",
            "CancelDate",
            "CreatedDate"
        ],

        "price": [
            "UnitPrice",
            "NetPrice",
            "PriceAfterDiscount"
        ],

        "top product": [
            "Highest TotalNetAmount",
            "Highest Quantity"
        ]
    }

    for term, options in known_terms.items():

        if f" {term} " in question_padded:

            detected_issues.append(
                {
                    "type": "ambiguity",
                    "term": term,
                    "options": options
                }
            )

            return detected_issues

    # --------------------------------------------------
    # Time Validation
    # --------------------------------------------------
    time_spec_found = False

    time_keywords = [
        "year",
        "fy",
        "last year",
        "this year",
        "current year",
        "ytd",
        "mtd",
        "qtd",
        "month",
        "quarter",
        "week"
    ]

    for keyword in time_keywords:
        if keyword in question_padded:
            time_spec_found = True
            break

    # --------------------------------------------------
    # Explicit Year Detection
    # --------------------------------------------------
    if not time_spec_found:

        potential_years = re.findall(
            r"\b\d{4}\b",
            question
        )

        current_year = datetime.datetime.now().year

        for year_str in potential_years:

            year = int(year_str)

            if 2019 < year <= current_year + 1:
                time_spec_found = True
                break

    # --------------------------------------------------
    # Missing Year
    # --------------------------------------------------
    if not time_spec_found:

        detected_issues.append(
            {
                "type": "year",
                "message":
                    "No time period specified. "
                    "Please specify YEAR or FY "
                    "(Example: FY2025 or 2025)."
            }
        )

    return detected_issues
